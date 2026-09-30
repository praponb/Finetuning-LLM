SELECT session_id, page, TO_DATE(event_time) AS event_time_date
FROM web_events
WHERE session_id IN (12, 27);
