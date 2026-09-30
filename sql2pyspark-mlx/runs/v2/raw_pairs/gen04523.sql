SELECT id, order_id, amount, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY amount) AS dense_rnk FROM payments
