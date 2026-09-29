SELECT event_type, HOUR(event_time) AS event_time_hour FROM web_events ORDER BY event_time_hour DESC;
