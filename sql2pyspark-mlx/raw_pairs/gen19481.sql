SELECT s.customer_id, c.segment, c.credit_limit, CASE WHEN s.device = 'tablet' THEN 1 ELSE 0 END AS is_tablet FROM sessions s RIGHT JOIN customers c ON c.id = s.customer_id
