SELECT YEAR(event_time) AS event_year, COUNT(DISTINCT duration_sec) AS unique_event_types FROM web_events WHERE session_id = 35 AND event_time < '2024-02-01' GROUP BY 1;
