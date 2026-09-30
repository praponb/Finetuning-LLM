SELECT o.id, o.discount, t.avg_amount FROM orders o INNER JOIN (SELECT order_id, ROUND(AVG(amount), 2) AS avg_amount FROM payments GROUP BY order_id) t ON o.id = t.order_id WHERE t.avg_amount > 67;
