SELECT s.id, s.customer_id, s.country FROM sessions s WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_time < '2023-04-01') ORDER BY s.customer_id DESC, country
