WITH kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM promotions
    INTERSECT ALL
    SELECT id
    FROM promotions
    WHERE promo_code IN ('WELCOME5', 'SPRING10', 'VIP')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN kept_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount_pct >= 0.35 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_tier,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    COUNT(DISTINCT channel) AS distinct_channels,
    COUNT(*) AS num_rows,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev
FROM verified
GROUP BY CASE WHEN discount_pct >= 0.35 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY num_rows DESC NULLS FIRST;
