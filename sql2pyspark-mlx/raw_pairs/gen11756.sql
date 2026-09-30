WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    APPROX_PERCENTILE(session_start_flag, 0.5) AS session_start_flag_p50,
    MAX(started_at) AS last_started_at,
    PERCENTILE_APPROX(session_no, ARRAY(0.25, 0.5, 0.75)) AS session_no_quartiles
FROM islands
GROUP BY quarter_start
ORDER BY quarter_start NULLS LAST
