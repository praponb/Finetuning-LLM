SELECT DATE_TRUNC('MONTH', o.order_date) AS order_month, SUM(oi.unit_price * oi.quantity) AS revenue
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.order_id = 7
GROUP BY DATE_TRUNC('MONTH', o.order_date)
ORDER BY order_month;
