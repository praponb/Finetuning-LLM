WITH product_stats AS (
    SELECT product_id, AVG(quantity) AS avg_quantity
    FROM inventory
    WHERE store_id IN (4, 6)
    GROUP BY product_id
)
SELECT p.id, p.supplier_id, ps.avg_quantity
FROM products p
LEFT OUTER JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.avg_quantity >= 110;
