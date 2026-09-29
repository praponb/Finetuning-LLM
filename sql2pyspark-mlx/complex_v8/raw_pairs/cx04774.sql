WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE source = 'radio'
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        device,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played_bucket) AS session_ms_played_bucket,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY device, burst_id
)
SELECT *
FROM (
    SELECT n_events, session_start, device, burst_id
    FROM per_session
) src
PIVOT (
    COUNT(burst_id)
    FOR device IN ('desktop', 'speaker', 'car')
)
ORDER BY n_events, session_start
