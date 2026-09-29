SELECT o.channel, MONTH(o.order_date) AS order_month, COUNT(*) AS cnt
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
FULL OUTER JOIN products p ON oi.product_id = p.id
GROUP BY o.channel, MONTH(o.order_date);
