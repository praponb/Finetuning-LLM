SELECT id, order_id, paid_at, method, amount, RANK() OVER (PARTITION BY order_id ORDER BY paid_at) rnk, AVG(amount) OVER (PARTITION BY order_id, method) order_id_avg_amount FROM payments
