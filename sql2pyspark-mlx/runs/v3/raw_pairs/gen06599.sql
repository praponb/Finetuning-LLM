SELECT event_type, DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_time_label FROM web_events WHERE session_id IN (15, 22, 27, 39);
