WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device
    FROM plays
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Subtotals with GROUPING SETS
SELECT
    new_session_flag,
    COALESCE(device, 'ALL') AS device,
    ARRAY_SORT(COLLECT_SET(device)) AS device_list,
    ROUND(SUM(ms_played) / NULLIF(SUM(session_id), 0), 3) AS ms_played_to_session_id_ratio
FROM sessionized
GROUP BY GROUPING SETS ((new_session_flag, device), (new_session_flag), ())
ORDER BY new_session_flag NULLS LAST, device NULLS LAST
