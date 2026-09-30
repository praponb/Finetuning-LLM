WITH filtered_products AS (
    SELECT *
    FROM products
    WHERE cost >= 260
),
products_stats AS (
    SELECT category, COUNT(stock_qty) AS stock_qty_count
    FROM filtered_products
    GROUP BY category
)
SELECT *
FROM products_stats
WHERE stock_qty_count > 9
