WITH recent_products AS (
    SELECT *
    FROM products
    WHERE supplier_id = 7
),
category_summary AS (
    SELECT category, MIN(price) AS min_price, COUNT(*) AS total_count
    FROM recent_products
    GROUP BY category
)
SELECT *
FROM category_summary
ORDER BY 1, total_count
