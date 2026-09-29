SELECT s.id, s.started_at, s.device FROM sessions s WHERE s.device IN ('mobile', 'tablet') AND s.customer_id IN (SELECT id FROM customers WHERE credit_limit < 10900) ORDER BY s.device DESC
