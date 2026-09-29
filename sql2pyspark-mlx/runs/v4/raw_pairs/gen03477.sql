WITH recent_products AS (
    SELECT *
    FROM products
    WHERE (cost BETWEEN 260 AND 620 AND stock_qty IS NULL) OR launch_date > '2024-08-04'
),
CategorySummary AS (
    SELECT category, ROUND(AVG(stock_qty), 2) AS avg_stock_qty, COUNT(CASE WHEN category = 'Electronics' THEN 1 END) AS electronics_count
    FROM recent_products
    GROUP BY category
)
SELECT category, avg_stock_qty, electronics_count
FROM CategorySummary
ORDER BY category, electronics_count DESC
