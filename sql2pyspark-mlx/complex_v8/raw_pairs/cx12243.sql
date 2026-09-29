WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM base_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT *
FROM (
    SELECT listener_id, track_id, device, burst_id
    FROM sessionized
) src
PIVOT (
    AVG(burst_id)
    FOR device IN ('car', 'tv')
)
ORDER BY listener_id, track_id
