SELECT s.device, s.country, we.page, we.event_type FROM sessions s LEFT JOIN web_events we ON s.id = we.session_id WHERE s.device = 'mobile' ORDER BY device;
