SELECT *, ROUND(duration_sec * 1.07, 2) adjusted_duration_sec, INITCAP(TRIM(event_type)) event_type_title
FROM web_events
WHERE HOUR(event_time) BETWEEN 6 AND 11 AND duration_sec <= 330
ORDER BY event_type_title DESC
