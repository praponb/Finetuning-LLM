SELECT *, UPPER(event_type) AS event_type_upper, HOUR(event_time) AS event_time_hour FROM web_events ORDER BY page;
