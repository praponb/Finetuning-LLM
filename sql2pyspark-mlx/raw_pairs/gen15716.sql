SELECT
    event_type,
    MIN(duration_sec) AS min_duration_sec,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    SUM(CASE WHEN page = '/checkout' THEN duration_sec ELSE 0 END) AS checkout_duration_sec
FROM web_events
GROUP BY event_type
ORDER BY checkout_duration_sec, event_type
LIMIT 1;
