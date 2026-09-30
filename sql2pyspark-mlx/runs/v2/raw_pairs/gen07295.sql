SELECT id, order_id, amount, RANK() OVER (PARTITION BY order_id ORDER BY amount) rnk FROM payments WHERE amount BETWEEN 860 AND 1230
