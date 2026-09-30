WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(price) OVER (PARTITION BY category, supplier_id ORDER BY price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS price_avg_6_prior,
        SUM(price) OVER w_part AS running_price,
        FIRST_VALUE(brand, TRUE) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_brand
    FROM products
    WINDOW w_part AS (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN price_avg_6_prior >= 1110 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_avg_6_prior_tier,
    supplier_id,
    APPROX_PERCENTILE(running_price, 0.9) AS running_price_p90,
    ROUND(SUM(price_avg_6_prior) / NULLIF(COUNT(CASE WHEN brand = 'Globex' THEN 1 END), 0), 2) AS price_avg_6_prior_per_globex
FROM with_windows
GROUP BY CASE WHEN price_avg_6_prior >= 1110 THEN 'PREMIUM' ELSE 'STANDARD' END, supplier_id
HAVING price_avg_6_prior_per_globex < 0
ORDER BY price_avg_6_prior_per_globex;
