SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    CASE WHEN price >= 1080 THEN 'LARGE' ELSE 'SMALL' END AS price_tier,
    PERCENTILE_APPROX(price, 0.9) AS price_p90,
    MAX(cost) AS max_cost,
    COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count
FROM products
GROUP BY week_start, price_tier;
