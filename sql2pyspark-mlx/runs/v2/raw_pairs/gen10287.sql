WITH recent_products AS (
    SELECT *
    FROM products
    WHERE supplier_id IS NOT NULL AND brand = 'Stark'
),
brand_summary AS (
    SELECT brand, SUM(CASE WHEN category = 'Home' THEN cost ELSE 0 END) AS home_cost, SUM(stock_qty) AS total_stock_qty
    FROM recent_products
    GROUP BY brand
)
SELECT brand, home_cost
FROM brand_summary
