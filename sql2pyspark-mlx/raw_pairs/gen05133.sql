WITH product_stats AS (
    SELECT product_id, COUNT(DISTINCT units) AS unique_stores, COUNT(units) AS units_count
    FROM store_sales
    WHERE store_id IN (1, 2, 3, 4)
    GROUP BY product_id
)
SELECT
    p.id,
    p.cost,
    p.stock_qty,
    ps.unique_stores,
    ps.units_count
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.units_count > 3;
