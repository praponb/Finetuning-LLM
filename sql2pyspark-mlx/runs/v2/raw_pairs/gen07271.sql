WITH recent_products AS (
    SELECT *
    FROM products
    WHERE launch_date BETWEEN '2023-06-01' AND '2025-05-01'
),
agg AS (
    SELECT category, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty, COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count
    FROM recent_products
    GROUP BY category
)
SELECT category, stark_count
FROM agg
WHERE stark_count >= 7
ORDER BY category;
