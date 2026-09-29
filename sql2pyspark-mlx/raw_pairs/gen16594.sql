SELECT s.id, s.device, c.credit_limit, c.country FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id ORDER BY s.device
