SELECT
    DATE_FORMAT(event_time, 'yyyy-MM') AS event_month,
    page,
    ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec,
    MAX(event_time) AS latest_event_time
FROM web_events
WHERE session_id = 32 OR event_type = 'page_view'
GROUP BY DATE_FORMAT(event_time, 'yyyy-MM'), page
HAVING AVG(duration_sec) >= 210;
