SELECT c.country, s.device, s.country AS session_country
FROM customers c
JOIN sessions s ON c.id = s.customer_id
ORDER BY c.country DESC;
