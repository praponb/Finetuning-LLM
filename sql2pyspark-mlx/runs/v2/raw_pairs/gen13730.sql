SELECT id, order_date, amount, LAG(amount, 2, 0) OVER (ORDER BY order_date, id) AS prev_amount FROM orders;
