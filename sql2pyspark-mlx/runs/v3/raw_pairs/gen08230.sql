SELECT id, event_time, session_id, TO_DATE(event_time) AS event_time_date FROM web_events WHERE duration_sec = 438 OR page = '/cart'
