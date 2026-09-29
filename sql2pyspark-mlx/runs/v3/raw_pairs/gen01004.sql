SELECT we.id, we.event_type, we.session_id, s.started_at, c.signup_date, c.segment FROM web_events we FULL OUTER JOIN sessions s ON we.session_id = s.id JOIN customers c ON s.customer_id = c.id;
