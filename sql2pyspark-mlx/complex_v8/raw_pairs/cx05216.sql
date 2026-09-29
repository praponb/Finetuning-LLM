WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE is_skipped = FALSE
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        track_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        SUM(ms_played_bucket) AS session_ms_played_bucket,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, burst_id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 60, 1) AS minutes_session_start_to_session_end,
        DATE_TRUNC('DAY', session_end) AS session_end_day
    FROM per_session
    WHERE burst_id > 2
),
enriched AS (
    -- Derive helper columns
    SELECT *, FLOOR(duration_min / 50) * 50 AS duration_min_bucket
    FROM derived
)
-- Final projection
SELECT
    track_id,
    burst_id,
    session_start,
    session_end,
    events,
    session_ms_played_bucket,
    duration_min,
    session_end_day,
    duration_min_bucket
FROM enriched
ORDER BY events DESC
