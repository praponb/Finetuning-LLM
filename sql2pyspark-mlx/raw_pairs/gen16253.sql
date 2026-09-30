SELECT order_id, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY order_id) AS cumulative_amount
FROM payments
WHERE paid_at >= '2024-04-10'
GROUP BY order_id;
