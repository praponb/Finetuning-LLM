SELECT id, customer_id, order_date, amount, LAG(amount, 1, 0) OVER (PARTITION BY customer_id ORDER BY order_date, id) prev_amount FROM orders ORDER BY id;
