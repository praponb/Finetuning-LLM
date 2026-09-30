SELECT we.page, s.country, c.credit_limit, c.name FROM web_events we INNER JOIN sessions s ON we.session_id = s.id INNER JOIN customers c ON c.id = s.customer_id;
