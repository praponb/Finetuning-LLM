SELECT s.browser, we.page, we.event_time FROM sessions s LEFT OUTER JOIN web_events we ON s.id = we.session_id;
