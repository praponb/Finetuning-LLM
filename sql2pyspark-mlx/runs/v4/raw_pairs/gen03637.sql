WITH filtered_products AS (
    SELECT *, LOWER(product_name) product_name_lower
    FROM products
    WHERE launch_date <= '2024-06-06' AND (cost BETWEEN 450 AND 560 OR cost <= 360)
),
products_stats AS (
    SELECT category, SUM(CASE WHEN category = 'Electronics' THEN 1 ELSE 0 END) electronics_count, SUM(cost) total_cost
    FROM filtered_products
    GROUP BY category
)
SELECT category, electronics_count, total_cost
FROM products_stats
