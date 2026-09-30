WITH valid_products AS (
    SELECT *
    FROM products
    WHERE SIZE(tags) > 1
),
ProductsStats AS (
    SELECT supplier_id, SUM(CASE WHEN category = 'Home' THEN 1 ELSE 0 END) AS home_count, COUNT(stock_qty) AS stock_qty_count
    FROM valid_products
    GROUP BY supplier_id
)
SELECT supplier_id, home_count, stock_qty_count
FROM ProductsStats
WHERE home_count >= 26
ORDER BY supplier_id DESC;
