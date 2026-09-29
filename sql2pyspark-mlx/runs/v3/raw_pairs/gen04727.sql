SELECT event_type, event_time, DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time FROM web_events
