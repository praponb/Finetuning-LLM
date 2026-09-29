SELECT p.category, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
WHERE o.status = 'CANCELLED'
GROUP BY p.category
ORDER BY stddev_quantity DESC
