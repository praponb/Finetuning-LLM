SELECT TO_DATE(event_time) AS event_day, MIN(event_time) AS min_event_time FROM web_events GROUP BY 1 ORDER BY event_day ASC LIMIT 5
