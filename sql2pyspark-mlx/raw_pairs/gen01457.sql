SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    CASE WHEN duration_sec >= 413 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    COUNT_IF(event_type = 'purchase') AS purchase_events,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    MAX(duration_sec) FILTER (WHERE page = '/product') AS product_max_duration_sec,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN duration_sec ELSE 0 END) AS add_to_cart_duration_sec
FROM web_events
GROUP BY DATE_TRUNC('YEAR', event_time), CASE WHEN duration_sec >= 413 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY year_start DESC NULLS LAST
