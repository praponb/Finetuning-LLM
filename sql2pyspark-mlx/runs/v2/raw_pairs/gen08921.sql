WITH product_stats AS (
    SELECT product_id, COUNT(DISTINCT unit_price) AS unique_products, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM order_items
    WHERE unit_price <= 160
    GROUP BY product_id
)
SELECT
    p.id,
    p.price,
    ps.unique_products,
    ps.avg_quantity
FROM products p
JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.avg_quantity >= 51
ORDER BY avg_quantity DESC, price;
