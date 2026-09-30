SELECT id, status, order_date, amount, LAG(amount, 2) OVER (PARTITION BY status ORDER BY order_date, id) AS prev_amount FROM orders
