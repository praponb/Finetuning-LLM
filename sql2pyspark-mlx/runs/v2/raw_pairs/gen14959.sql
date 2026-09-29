SELECT id, order_id, method, amount, ROUND(amount / SUM(amount) OVER (PARTITION BY order_id, method), 3) AS amount_share FROM payments;
