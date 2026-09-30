WITH gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessions_sessions AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    CASE WHEN island_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    APPROX_PERCENTILE(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    AVG(island_id) AS avg_island_id,
    COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count
FROM sessions_sessions
GROUP BY ROLLUP (CASE WHEN island_id >= 7 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', started_at));
