WITH prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) AS prev_started_at
    FROM sessions
),
with_gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('WEEK', prev_started_at) AS week_start,
    DATE_TRUNC('MONTH', prev_started_at) AS activity_month,
    customer_id,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    PERCENTILE_APPROX(burst_id, 0.95) AS burst_id_p95,
    ROUND(SUM(burst_id) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS burst_id_per_de,
    ROUND(AVG(burst_id), 2) AS avg_burst_id,
    MIN(prev_started_at) AS first_seen,
    ROUND(AVG(new_group_flag), 2) AS avg_new_group_flag,
    GROUPING(DATE_TRUNC('WEEK', prev_started_at)) AS grouping_id
FROM sessionized
GROUP BY ROLLUP (DATE_TRUNC('WEEK', prev_started_at), DATE_TRUNC('MONTH', prev_started_at), customer_id)
ORDER BY week_start ASC;
