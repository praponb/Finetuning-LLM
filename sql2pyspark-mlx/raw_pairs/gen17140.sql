WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    CASE WHEN price >= 1130 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    MAX(price) FILTER (WHERE category = 'Fashion') AS fashion_max_price,
    CORR(cost, price) AS corr_cost_price,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
    CORR(cost, price) AS corr_cost_price_2,
    COUNT(*) FILTER (WHERE category = 'Grocery') AS grocery_rows,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
    GROUPING(DATE_TRUNC('WEEK', launch_date)) AS grouping_id
FROM not_flagged
GROUP BY ROLLUP (DATE_TRUNC('WEEK', launch_date), CASE WHEN price >= 1130 THEN 'HIGH' ELSE 'LOW' END);
