SELECT page, MONTH(event_time) event_month, MIN(event_time) first_event_time
FROM web_events
GROUP BY 1, 2
