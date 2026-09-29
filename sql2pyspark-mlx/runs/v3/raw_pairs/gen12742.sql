SELECT s.country, s.browser, we.page FROM sessions s JOIN web_events we ON we.session_id = s.id WHERE we.session_id IN (20, 25, 30)
