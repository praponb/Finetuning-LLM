WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM prepared_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        listener_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, burst_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(session_start, 'yyyy-MM') AS session_start_label,
        DATE_TRUNC('QUARTER', session_end) AS session_end_quarter,
        DATEDIFF(DATE '2026-01-01', session_start) AS days_since_session_start
    FROM sessions
)
-- Subtotals with ROLLUP
SELECT
    listener_id,
    event_count,
    burst_id,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids,
    PERCENTILE_APPROX(duration_min, 0.5) AS median_duration_min
FROM labeled
GROUP BY ROLLUP(listener_id, event_count, burst_id)
