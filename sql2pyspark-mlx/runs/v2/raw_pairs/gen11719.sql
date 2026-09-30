SELECT event_time, LENGTH(page) AS page_length FROM web_events WHERE duration_sec < 257 OR event_time < '2024-06-11' ORDER BY event_time;
