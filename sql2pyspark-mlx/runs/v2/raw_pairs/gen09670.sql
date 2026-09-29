SELECT
    page,
    HOUR(event_time) event_hour,
    DATE_TRUNC('MONTH', event_time) event_month,
    SUM(duration_sec) total_duration_sec
FROM web_events
WHERE HOUR(event_time) BETWEEN 9 AND 12 AND event_time < '2025-03-01'
GROUP BY page, HOUR(event_time), DATE_TRUNC('MONTH', event_time)
HAVING COUNT(duration_sec) < 2
ORDER BY event_month DESC;
