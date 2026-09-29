SELECT o.status, COUNT(oi.id) AS order_item_count, ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM orders o
LEFT JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.status
ORDER BY order_item_count, o.status DESC;
