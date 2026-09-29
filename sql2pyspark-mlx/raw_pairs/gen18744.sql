WITH filtered_products AS (
    SELECT *, ROUND(stock_qty / cost, 2) AS stock_qty_per_cost
    FROM products
    WHERE (SIZE(tags) >= 1 AND cost >= 570) OR ARRAY_CONTAINS(tags, 'clearance')
),
LaunchMonthSummary AS (
    SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, SUM(CASE WHEN category = 'Sports' THEN stock_qty ELSE 0 END) AS sports_stock_qty
    FROM filtered_products
    GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM')
)
SELECT launch_month, sports_stock_qty
FROM LaunchMonthSummary
WHERE sports_stock_qty >= 25
ORDER BY sports_stock_qty ASC, launch_month ASC
LIMIT 50
