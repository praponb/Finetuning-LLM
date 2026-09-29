WITH kept_keys AS (
    -- Set intersection on keys
    SELECT channel
    FROM promotions
    INTERSECT
    SELECT channel
    FROM promotions
    WHERE ends_on < '2023-11-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN kept_keys k ON s.channel = k.channel
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount_pct >= 0.36 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    COUNT(*) AS cnt,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    AVG(discount_pct) FILTER (WHERE promo_code = 'SPRING10') AS spring10_avg_discount_pct,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS discount_pct_per_partner,
    SUM(CASE WHEN channel = 'web' THEN discount_pct ELSE 0 END) AS web_discount_pct,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct
FROM kept_rows
GROUP BY 1;
