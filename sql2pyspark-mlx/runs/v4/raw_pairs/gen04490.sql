SELECT
    CASE WHEN price >= 1080 THEN 'LARGE' ELSE 'SMALL' END AS price_tier,
    brand,
    PERCENTILE_APPROX(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles,
    COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count
FROM products
GROUP BY GROUPING SETS ((CASE WHEN price >= 1080 THEN 'LARGE' ELSE 'SMALL' END, brand), (CASE WHEN price >= 1080 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY brand DESC
