WITH base AS (
    SELECT *
    FROM products
    WHERE launch_date < '2024-10-12' AND supplier_id IN (11, 16, 32) AND price <= 640
),
products_stats AS (
    SELECT brand, SUM(CASE WHEN category = 'Books' THEN stock_qty ELSE 0 END) AS books_stock_qty
    FROM base
    GROUP BY brand
)
SELECT *
FROM products_stats
WHERE books_stock_qty < 22
