SELECT we.page, we.event_type, s.country, s.customer_id, c.segment FROM web_events we INNER JOIN sessions s ON s.id = we.session_id FULL OUTER JOIN customers c ON s.customer_id = c.id
