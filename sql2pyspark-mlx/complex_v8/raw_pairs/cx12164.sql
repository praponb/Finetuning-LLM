WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
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
        COUNT(*) AS event_count,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY device, burst_id
)
-- Final projection
SELECT
    device,
    session_start,
    session_end,
    event_count,
    session_ms_played,
    duration_min,
    ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 3600, 1) AS hours_session_end_to_session_start,
    DATE_FORMAT(session_start, 'yyyy-MM-dd HH:00') AS session_start_label
FROM per_session
WHERE duration_min BETWEEN 24.0 AND 228.0
ORDER BY session_end NULLS LAST
