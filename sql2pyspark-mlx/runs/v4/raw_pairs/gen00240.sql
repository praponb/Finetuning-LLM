SELECT
    event_type,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/cart' THEN 1 END), 0), 2) AS duration_sec_per_cart,
    MIN(duration_sec) AS min_duration_sec
FROM web_events
GROUP BY event_type, DATE_TRUNC('YEAR', event_time)
