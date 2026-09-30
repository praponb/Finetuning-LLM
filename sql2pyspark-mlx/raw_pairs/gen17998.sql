WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        currency,
        tier_limits,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    *,
    AVG(avg_tier_price) OVER w_grp AS last4_avg_avg_tier_price,
    SUM(avg_tier_price) OVER w_grp AS last4_sum_avg_tier_price,
    COUNT(avg_tier_price) OVER w_grp AS last4_count_avg_tier_price,
    MIN(avg_tier_price) OVER w_grp AS last4_min_avg_tier_price,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY avg_tier_price DESC NULLS FIRST) AS avg_tier_price_dense_rank
FROM unpacked
WHERE avg_tier_price <= 20
WINDOW w_grp AS (PARTITION BY currency ORDER BY avg_tier_price ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
