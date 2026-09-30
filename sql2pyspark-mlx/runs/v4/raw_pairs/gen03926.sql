WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY price DESC, id) AS price_bucket,
        ROW_NUMBER() OVER (ORDER BY stock_qty, id) AS row_seq
    FROM products
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN price_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_bucket_tier,
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count,
    SUM(CASE WHEN brand = 'Globex' THEN price_bucket ELSE 0 END) AS globex_price_bucket,
    PERCENTILE_APPROX(price, 0.5) AS price_p50
FROM scored
GROUP BY CASE WHEN price_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', launch_date);
