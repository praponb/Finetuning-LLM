SELECT order_id, status, SUM(amount) AS total_amount FROM payments GROUP BY 1, 2 HAVING total_amount > 5300;
