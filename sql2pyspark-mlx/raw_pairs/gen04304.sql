SELECT o.store_id, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN products p ON oi.product_id = p.id
GROUP BY o.store_id
ORDER BY store_id DESC;
