SELECT
    YEAR(p.launch_date) AS launch_year,
    p.brand,
    MAX(oi.unit_price) - MIN(oi.unit_price) AS unit_price_range,
    MAX(oi.quantity) AS max_quantity
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE p.cost < 480
GROUP BY YEAR(p.launch_date), p.brand
HAVING unit_price_range >= 65
ORDER BY max_quantity;
