SELECT DATE_TRUNC('MONTH', o.order_date) AS order_month, COUNT(*) AS row_count
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE oi.quantity <= 6
GROUP BY DATE_TRUNC('MONTH', o.order_date)
HAVING row_count > 4;
