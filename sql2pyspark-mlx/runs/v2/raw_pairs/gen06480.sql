SELECT
    p.brand,
    p.category,
    MIN(oi.unit_price) AS min_unit_price,
    ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
GROUP BY p.brand, p.category
ORDER BY avg_unit_price DESC
