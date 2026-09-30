SELECT c.city, COUNT(DISTINCT s.customer_id) AS unique_customers
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.device NOT IN ('desktop', 'mobile')
GROUP BY c.city
ORDER BY unique_customers;
