SELECT method, order_id, MIN(amount) AS min_amount FROM payments GROUP BY ROLLUP(method, order_id) ORDER BY order_id DESC
