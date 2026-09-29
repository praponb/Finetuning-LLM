SELECT
    s.city,
    s.region,
    ROUND(AVG(oi.quantity), 2) AS avg_quantity,
    COUNT(DISTINCT oi.unit_price) AS unique_products
FROM order_items oi
LEFT OUTER JOIN orders o ON o.id = oi.order_id
LEFT OUTER JOIN stores s ON o.store_id = s.id
WHERE oi.unit_price <= 350
GROUP BY s.city, s.region
ORDER BY city;
