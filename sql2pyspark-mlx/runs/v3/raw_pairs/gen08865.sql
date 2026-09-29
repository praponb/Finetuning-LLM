SELECT
    product_id,
    AVG(quantity) AS avg_quantity,
    SUM(quantity * unit_price) AS revenue,
    COUNT(unit_price) AS unit_price_count
FROM order_items
WHERE unit_price BETWEEN 120 AND 150 OR unit_price <= 240
GROUP BY product_id
