WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessions_web_events AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    APPROX_PERCENTILE(duration_sec, 0.5) AS duration_sec_p50,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    COUNT_IF(event_type = 'purchase') AS purchase_events
FROM sessions_web_events
GROUP BY DATE_TRUNC('MONTH', event_time), DATE_TRUNC('YEAR', event_time)
