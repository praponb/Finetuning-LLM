SELECT c.city, MAX(p.amount) AS max_amount
FROM payments p
LEFT OUTER JOIN orders o ON p.order_id = o.id
LEFT OUTER JOIN customers c ON c.id = o.customer_id
GROUP BY c.city
HAVING MAX(p.amount) > 640
ORDER BY city DESC
LIMIT 5
