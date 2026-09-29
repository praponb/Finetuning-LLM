WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM promotions
    QUALIFY RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END), 0), 2) AS discount_pct_per_welcome5,
    MAX(discount_pct) FILTER (WHERE channel = 'mobile') AS mobile_max_discount_pct,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    AVG(discount_pct) FILTER (WHERE channel = 'web') AS web_avg_discount_pct,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev_2
FROM latest
GROUP BY DATE_TRUNC('MONTH', ends_on);
