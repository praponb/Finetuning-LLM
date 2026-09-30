WITH with_gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessions_sessions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    COUNT(DISTINCT device) AS distinct_devices,
    SUM(session_id) FILTER (WHERE device = 'tablet') AS tablet_total_session_id,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN browser = 'Safari' THEN 1 END), 0), 2) AS session_id_per_safari
FROM sessions_sessions
GROUP BY DATE_TRUNC('WEEK', started_at), DATE_TRUNC('MONTH', started_at)
