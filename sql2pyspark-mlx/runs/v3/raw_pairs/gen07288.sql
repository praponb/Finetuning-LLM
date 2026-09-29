SELECT status, COUNT(amount) AS amount_count, AVG(discount) AS avg_discount
FROM orders
GROUP BY status
HAVING MAX(discount) > 0.22
ORDER BY status;
