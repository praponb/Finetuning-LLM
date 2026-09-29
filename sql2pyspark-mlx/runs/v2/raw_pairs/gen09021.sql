SELECT s.device, s.customer_id, c.email
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
