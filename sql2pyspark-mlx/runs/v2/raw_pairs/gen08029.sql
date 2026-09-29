SELECT c.city, MIN(o.amount) AS min_amount
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
INNER JOIN stores s ON o.store_id = s.id
GROUP BY c.city
HAVING MIN(o.amount) >= 1490
ORDER BY city ASC, min_amount DESC
LIMIT 1;
