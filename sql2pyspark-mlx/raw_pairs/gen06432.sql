WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        cost,
        stock_qty,
        tags,
        supplier_id,
        COALESCE(product_name, 'Unknown') AS product_name_filled,
        COALESCE(brand, 'DIRECT') AS brand_filled
    FROM products
    WHERE cost > 310
)
SELECT
    dim.country,
    dim.city,
    COUNT(*) AS cnt,
    AVG(a.cost) AS avg_cost,
    MIN(a.stock_qty) AS min_stock_qty,
    GROUPING_ID() AS level_id
FROM valid a
LEFT JOIN suppliers dim ON a.supplier_id = dim.id
GROUP BY CUBE (dim.country, dim.city)
ORDER BY min_stock_qty DESC;
