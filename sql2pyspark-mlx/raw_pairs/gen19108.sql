SELECT order_id, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY order_id) AS cumulative_amount FROM payments GROUP BY order_id ORDER BY 2;
