SELECT
    CASE WHEN stock_qty >= 403 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_tier,
    category,
    COUNT(DISTINCT category) AS distinct_categories,
    COUNT(*) FILTER (WHERE category = 'Electronics') AS electronics_rows,
    PERCENTILE_APPROX(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles,
    COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count,
    SUM(price) AS total_price,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN stock_qty >= 403 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(price) DESC) AS total_price_dense_rank
FROM products
GROUP BY CASE WHEN stock_qty >= 403 THEN 'PREMIUM' ELSE 'STANDARD' END, category
HAVING distinct_categories > 1
ORDER BY electronics_rows;
