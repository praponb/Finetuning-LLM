SELECT s.region, s.city, ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
JOIN stores s ON o.store_id = s.id
WHERE oi.product_id IN (9, 23, 26, 29)
GROUP BY s.region, s.city
ORDER BY s.city, region
LIMIT 50
