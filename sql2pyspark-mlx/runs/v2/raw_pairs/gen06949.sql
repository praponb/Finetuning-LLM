SELECT order_id, AVG(amount) AS avg_amount, SUM(amount) AS total_amount
FROM payments
WHERE order_id IS NULL
GROUP BY order_id;
