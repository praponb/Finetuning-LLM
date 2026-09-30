SELECT o.customer_id, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity, AVG(oi.unit_price) AS avg_unit_price
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
GROUP BY o.customer_id
ORDER BY stddev_quantity DESC
