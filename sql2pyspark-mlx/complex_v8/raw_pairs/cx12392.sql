WITH filtered_plays AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE ms_played <= 225733
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        track_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, burst_id
),
stats AS (
    -- Summarise per group
    SELECT
        burst_id,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        SUM(events) AS total_events,
        PERCENTILE_APPROX(events, 0.5) AS median_events,
        ROUND(STDDEV(burst_id), 2) AS stddev_burst_id,
        MAX(session_end) AS last_session_end
    FROM session_summary
    GROUP BY burst_id, DATE_TRUNC('MONTH', session_start), DATE_TRUNC('MONTH', session_end)
)
SELECT
    burst_id,
    session_start_month,
    session_end_month,
    stddev_burst_id,
    last_session_end,
    ROUND(stddev_burst_id * 1.5, 2) AS stddev_burst_id_adj
FROM stats
ORDER BY stddev_burst_id DESC, session_end_month
