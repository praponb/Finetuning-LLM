SELECT s.device, c.segment, c.email
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id;
