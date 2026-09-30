SELECT s.started_at, s.customer_id, we.page, we.event_time FROM sessions s INNER JOIN web_events we ON we.session_id = s.id WHERE we.event_type IN ('click', 'search') ORDER BY event_time
