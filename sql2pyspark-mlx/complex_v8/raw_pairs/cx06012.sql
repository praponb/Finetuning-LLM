WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref
    FROM plays
    WHERE track_id >= 29
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        track_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, session_id
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        n_events,
        MIN(session_end) AS first_session_end,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        MAX(n_events) AS max_n_events,
        ROUND(SUM(n_events) / NULLIF(SUM(session_id), 0), 3) AS n_events_to_session_id_ratio,
        PERCENTILE_APPROX(session_id, 0.5) AS median_session_id
    FROM session_summary
    WHERE duration_min > 72.0
    GROUP BY DATE_TRUNC('MONTH', session_end), n_events
)
SELECT
    session_end_month,
    n_events,
    distinct_track_ids,
    max_n_events,
    median_session_id,
    FLOOR(distinct_track_ids / 2) * 2 AS distinct_track_ids_bucket
FROM summary
WHERE TO_DATE(first_session_end) < DATE '2025-01-25'
ORDER BY max_n_events DESC
