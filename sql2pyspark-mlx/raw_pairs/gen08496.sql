SELECT *, CEIL(duration_sec) AS duration_sec_whole, TO_DATE(event_time) AS event_time_date FROM web_events ORDER BY event_time DESC;
