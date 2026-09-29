SELECT s.customer_id, we.event_time, we.event_type, we.session_id FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE s.browser IN ('Safari', 'Firefox', 'Edge')
