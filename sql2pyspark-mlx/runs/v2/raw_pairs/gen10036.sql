SELECT we.event_type, we.duration_sec, s.started_at, c.signup_date, c.email FROM web_events we JOIN sessions s ON s.id = we.session_id JOIN customers c ON s.customer_id = c.id;
