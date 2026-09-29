SELECT id, event_time, DATEDIFF(CURRENT_DATE(), event_time) days_since_event_time, DATE_FORMAT(event_time, 'yyyy-MM') event_time_label FROM web_events;
