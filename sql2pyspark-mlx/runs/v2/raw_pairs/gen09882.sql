SELECT *, DAYOFWEEK(event_time) AS event_time_dayofweek, DAY(event_time) AS event_time_day
FROM web_events
WHERE duration_sec BETWEEN 270 AND 440 AND (TO_DATE(event_time) < '2023-09-22' OR event_type IN ('click', 'purchase'));
