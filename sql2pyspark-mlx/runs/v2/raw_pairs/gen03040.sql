SELECT id, order_date, amount, LAG(amount, 1, 0) OVER (ORDER BY order_date, id) AS prev_amount FROM orders WHERE discount BETWEEN 0.19 AND 0.25;
