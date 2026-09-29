SELECT s.region, AVG(oi.quantity) AS avg_quantity, MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN stores s ON o.store_id = s.id
WHERE o.shipping_fee <= 28
GROUP BY s.region
ORDER BY region
LIMIT 3;
