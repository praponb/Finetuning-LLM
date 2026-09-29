WITH product_summary AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_stores, MAX(last_restock) AS latest_last_restock
    FROM inventory
    WHERE quantity != 207
    GROUP BY product_id
)
SELECT
    p.id,
    p.brand,
    p.supplier_id,
    ps.unique_stores,
    ps.latest_last_restock
FROM products p
JOIN product_summary ps ON p.id = ps.product_id
ORDER BY p.brand
