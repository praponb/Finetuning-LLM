SELECT event_type, YEAR(event_time) event_year, MAX(duration_sec) - MIN(duration_sec) duration_sec_range
FROM web_events
GROUP BY event_type, YEAR(event_time)
ORDER BY event_year DESC, event_type
LIMIT 5;
