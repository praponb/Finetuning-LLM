WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE TO_DATE(opened_at) < DATE '2025-02-18'
      AND requester_id >= 5
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        closed_at,
        RANK() OVER (ORDER BY closed_at DESC, id) AS closed_at_rank,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel
    FROM recent
),
leaders AS (
    -- First row per group
    SELECT
        id,
        requester_id,
        closed_at,
        rows_in_channel
    FROM with_windows
    WHERE closed_at_rank = 1
      AND MONTH(closed_at) IN (1, 2)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(requester_id / NULLIF(rows_in_channel, 0), 3) AS requester_id_per_rows_in_channel
    FROM leaders
)
SELECT
    id,
    requester_id,
    rows_in_channel,
    requester_id_per_rows_in_channel
FROM enriched
ORDER BY requester_id, id DESC NULLS FIRST
