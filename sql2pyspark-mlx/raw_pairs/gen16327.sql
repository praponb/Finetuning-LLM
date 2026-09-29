SELECT id, customer_id, order_date, amount, SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date, id) running_amount FROM orders WHERE customer_id IS NOT NULL
