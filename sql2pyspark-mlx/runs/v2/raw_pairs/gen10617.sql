SELECT session_id, TO_DATE(event_time) AS event_time_date
FROM web_events
ORDER BY event_time_date;
