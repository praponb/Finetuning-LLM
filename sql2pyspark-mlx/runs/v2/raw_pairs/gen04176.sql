SELECT s.device, s.started_at, c.credit_limit FROM sessions s INNER JOIN customers c ON c.id = s.customer_id ORDER BY credit_limit DESC
