SELECT event_type, MIN(event_time) AS min_event_time FROM web_events GROUP BY event_type HAVING COUNT(*) > 2 ORDER BY min_event_time, event_type LIMIT 5;
