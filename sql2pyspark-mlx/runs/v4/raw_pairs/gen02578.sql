SELECT s.customer_id, s.browser, s.started_at, we.event_type FROM sessions s INNER JOIN web_events we ON s.id = we.session_id WHERE we.event_type IN ('add_to_cart', 'click', 'page_view');
