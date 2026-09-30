SELECT c.email, s.customer_id
FROM customers c
LEFT JOIN sessions s ON c.id = s.customer_id
