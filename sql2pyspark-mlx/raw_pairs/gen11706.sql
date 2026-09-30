WITH RecentProducts AS (
    SELECT *, COALESCE(stock_qty, 0) AS stock_qty_filled
    FROM products
    WHERE stock_qty BETWEEN 200 AND 283 AND stock_qty <> 193
),
agg AS (
    SELECT category, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM RecentProducts
    GROUP BY category
)
SELECT *
FROM agg
WHERE stddev_cost > 27
ORDER BY category DESC
LIMIT 5;
