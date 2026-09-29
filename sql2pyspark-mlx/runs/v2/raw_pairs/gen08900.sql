SELECT c.city, MAX(o.amount) AS max_amount, MAX(o.amount) - MIN(o.amount) AS amount_range
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY c.city
ORDER BY amount_range
