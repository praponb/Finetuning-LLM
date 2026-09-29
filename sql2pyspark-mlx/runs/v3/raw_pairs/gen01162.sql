SELECT *
FROM web_events
WHERE event_time >= '2024-11-01' AND HOUR(event_time) BETWEEN 7 AND 15
