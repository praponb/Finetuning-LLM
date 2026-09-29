SELECT page, HOUR(event_time) AS event_time_hour FROM web_events WHERE HOUR(event_time) BETWEEN 9 AND 13;
