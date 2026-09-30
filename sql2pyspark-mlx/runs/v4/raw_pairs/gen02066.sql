WITH scored AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY category ORDER BY stock_qty DESC, id) stock_qty_bucket,
        AVG(price) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) price_avg_6
    FROM products
)
SELECT
    CASE WHEN price_avg_6 >= 903 THEN 'HIGH' ELSE 'LOW' END AS price_avg_6_tier,
    brand,
    AVG(price) AS avg_price,
    ROUND(AVG(cost), 2) AS avg_cost,
    PERCENTILE_APPROX(cost, 0.9) AS cost_p90,
    CORR(stock_qty, price) AS corr_stock_qty_price,
    COUNT_IF(category = 'Home') AS home_events,
    RANK() OVER (PARTITION BY CASE WHEN price_avg_6 >= 903 THEN 'HIGH' ELSE 'LOW' END ORDER BY CORR(stock_qty, price) DESC) AS corr_stock_qty_price_rank
FROM scored
GROUP BY price_avg_6_tier, brand
HAVING avg_cost >= 0
