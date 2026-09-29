WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref
    FROM plays
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        source,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_no
)
SELECT
    source,
    session_no,
    session_start,
    n_events,
    session_ms_played,
    duration_min,
    session_start + INTERVAL 30 MINUTES AS session_start_plus_30m
FROM per_session
WHERE MONTH(session_start) IN (1, 2)
ORDER BY session_no
