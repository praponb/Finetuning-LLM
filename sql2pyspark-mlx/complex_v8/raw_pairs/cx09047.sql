WITH cleaned AS (
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
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        listener_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_id
)
-- Final projection
SELECT
    listener_id,
    session_id,
    session_end,
    events,
    session_ms_played,
    duration_min,
    DATE_TRUNC('DAY', session_start) AS session_start_day,
    ROUND(session_id / NULLIF(duration_min, 0), 3) AS session_id_per_duration_min
FROM session_summary
ORDER BY listener_id NULLS LAST, session_start_day DESC
