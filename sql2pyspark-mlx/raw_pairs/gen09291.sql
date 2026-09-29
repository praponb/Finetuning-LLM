SELECT s.browser, s.device, we.page, we.event_time FROM sessions s JOIN web_events we ON we.session_id = s.id WHERE we.event_time >= '2025-01-01';
