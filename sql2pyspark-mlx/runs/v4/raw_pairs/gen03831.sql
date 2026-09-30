SELECT c.country, COUNT(DISTINCT o.discount) AS unique_customers
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY c.country
ORDER BY 2 DESC, country
LIMIT 3
