SELECT
    page,
    COUNT(DISTINCT duration_sec) AS unique_sessions,
    COUNT(DISTINCT duration_sec) AS unique_event_types,
    MIN(event_time) AS min_event_time
FROM web_events
WHERE event_type = 'purchase' AND event_type IN ('purchase', 'add_to_cart', 'click')
GROUP BY page
HAVING COUNT(DISTINCT duration_sec) > 2
ORDER BY unique_event_types DESC;
