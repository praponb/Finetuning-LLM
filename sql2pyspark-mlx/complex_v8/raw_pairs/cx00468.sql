WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM filtered_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        quality,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        quality,
        session_id,
        session_start,
        session_end,
        n_events,
        duration_min,
        MAX(session_id) OVER (PARTITION BY quality) AS quality_max_session_id,
        NTILE(5) OVER (PARTITION BY quality ORDER BY duration_min NULLS LAST, quality, session_id) AS duration_min_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY duration_min NULLS LAST, quality, session_id), 3) AS duration_min_cume_dist,
        LAST_VALUE(session_id) IGNORE NULLS OVER (PARTITION BY quality, TO_DATE(session_end) ORDER BY duration_min DESC, quality, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_id,
        MAX(duration_min) OVER (PARTITION BY quality) AS quality_max_duration_min
    FROM session_summary
)
-- Native SQL UNPIVOT (wide to long)
SELECT quality, session_id, field, metric_value
FROM (
    SELECT quality, session_id, duration_min_cume_dist, duration_min, quality_max_duration_min
    FROM with_trends
) src
UNPIVOT (
    metric_value FOR field IN (duration_min_cume_dist, duration_min, quality_max_duration_min)
) unp
