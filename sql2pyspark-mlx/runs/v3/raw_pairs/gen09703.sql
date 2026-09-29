SELECT customers.country, COUNT(DISTINCT sessions.customer_id) AS unique_customers
FROM sessions
JOIN customers ON sessions.customer_id = customers.id
GROUP BY customers.country
ORDER BY unique_customers DESC, customers.country
