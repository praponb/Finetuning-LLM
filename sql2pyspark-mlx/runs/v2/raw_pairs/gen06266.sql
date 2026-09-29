SELECT event_time, SUBSTRING(event_type, 1, 2) AS event_type_prefix, UPPER(event_type) AS event_type_upper
FROM web_events
