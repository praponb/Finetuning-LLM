SELECT o.status, COUNT(DISTINCT p.amount) AS unique_methods
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
GROUP BY o.status
ORDER BY unique_methods;
