SELECT event_type, HOUR(event_time) AS event_hour, COUNT(duration_sec) AS duration_sec_count
FROM web_events
WHERE event_type NOT IN ('page_view', 'search')
GROUP BY event_type, HOUR(event_time)
ORDER BY event_type;
