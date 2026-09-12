defmodule SymphonyElixir.BlockedStore do
  @moduledoc """
  Persists orchestrator blocks next to the configured workflow file.

  The tracker remains read-only: this store is provider-independent local state.
  """

  alias SymphonyElixir.Tracker.Issue
  alias SymphonyElixir.Workflow

  @filename ".symphony-blocked.json"
  @version 2

  @spec path() :: Path.t()
  def path do
    Workflow.workflow_file_path()
    |> Path.expand()
    |> Path.dirname()
    |> Path.join(@filename)
  end

  @spec load() :: {:ok, map()} | {:error, term()}
  def load do
    store_path = path()

    case File.read(store_path) do
      {:ok, contents} -> decode(contents, store_path)
      {:error, :enoent} -> {:ok, %{}}
      {:error, reason} -> {:error, {:blocked_store_read_failed, store_path, reason}}
    end
  end

  @spec persist(map()) :: :ok | {:error, term()}
  def persist(blocked) when is_map(blocked) do
    store_path = path()
    temporary_path = store_path <> ".tmp-#{System.unique_integer([:positive, :monotonic])}"

    payload = %{
      "version" => @version,
      "blocked" => blocked |> Map.values() |> Enum.map(&encode_entry/1) |> Enum.sort_by(& &1["issue_id"])
    }

    with {:ok, json} <- Jason.encode(payload, pretty: true),
         :ok <- File.write(temporary_path, json <> "\n", [:binary, :sync]),
         :ok <- File.rename(temporary_path, store_path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary_path)
        {:error, {:blocked_store_write_failed, store_path, reason}}
    end
  end

  defp decode(contents, store_path) do
    with {:ok, %{"version" => version, "blocked" => entries}} <- Jason.decode(contents),
         true <- version in [1, @version],
         true <- is_list(entries),
         {:ok, blocked} <- decode_entries(entries) do
      {:ok, blocked}
    else
      _ -> {:error, {:invalid_blocked_store, store_path}}
    end
  end

  defp decode_entries(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn entry, {:ok, blocked} ->
      case decode_entry(entry) do
        {:ok, issue_id, blocked_entry} -> {:cont, {:ok, Map.put(blocked, issue_id, blocked_entry)}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp decode_entry(
         %{
           "issue_id" => issue_id,
           "identifier" => identifier,
           "error" => error,
           "blocked_at" => blocked_at,
           "issue_updated_at" => issue_updated_at
         } = entry
       )
       when is_binary(issue_id) and is_binary(identifier) and is_binary(error) and
              (is_binary(issue_updated_at) or is_nil(issue_updated_at)) do
    status = Map.get(entry, "status", "blocked")

    with true <- status in ["inflight", "blocked"],
         {:ok, blocked_at} <- parse_datetime(blocked_at),
         {:ok, issue_updated_at} <- parse_optional_datetime(issue_updated_at) do
      issue = %Issue{id: issue_id, identifier: identifier, updated_at: issue_updated_at}

      {:ok, issue_id,
       %{
         issue_id: issue_id,
         identifier: identifier,
         issue: issue,
         worker_host: nil,
         workspace_path: nil,
         session_id: nil,
         error: error,
         status: String.to_existing_atom(status),
         blocked_at: blocked_at,
         issue_updated_at: issue_updated_at,
         last_codex_message: nil,
         last_codex_event: nil,
         last_codex_timestamp: nil
       }}
    else
      _ -> :error
    end
  end

  defp decode_entry(_entry), do: :error

  defp encode_entry(entry) do
    %{
      "issue_id" => entry.issue_id,
      "identifier" => entry.identifier,
      "error" => entry.error,
      "status" => entry |> Map.get(:status, :blocked) |> Atom.to_string(),
      "blocked_at" => datetime_to_iso8601(entry.blocked_at),
      "issue_updated_at" => datetime_to_iso8601(Map.get(entry, :issue_updated_at))
    }
  end

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_datetime(_value), do: :error

  defp parse_optional_datetime(nil), do: {:ok, nil}
  defp parse_optional_datetime(value) when is_binary(value), do: parse_datetime(value)

  defp datetime_to_iso8601(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp datetime_to_iso8601(nil), do: nil
end
