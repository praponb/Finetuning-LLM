SELECT p.brand, SUM(oi.quantity) AS total_quantity, MAX(oi.unit_price) - MIN(oi.unit_price) AS unit_price_range
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON p.id = oi.product_id
GROUP BY p.brand
HAVING SUM(oi.quantity) > 17
ORDER BY unit_price_range DESC
