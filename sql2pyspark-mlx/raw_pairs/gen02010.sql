SELECT s.browser, s.device, s.customer_id, we.event_time, LOWER(we.page) page_lower FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id;
