WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity IS NOT NULL
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    brand,
    CASE WHEN price >= 1240 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    MAX(price) FILTER (WHERE category = 'Sports') AS sports_max_price,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN category = 'Grocery' THEN 1 END), 0), 2) AS stock_qty_per_grocery,
    GROUPING(DATE_TRUNC('YEAR', launch_date)) AS grouping_id
FROM eligible
GROUP BY CUBE (DATE_TRUNC('YEAR', launch_date), brand, CASE WHEN price >= 1240 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY stock_qty_per_grocery NULLS LAST
