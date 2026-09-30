WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        tier_limits,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    id,
    avg_tier_price,
    lowest_price,
    MAX(avg_tier_price) OVER (ORDER BY lowest_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_avg_tier_price,
    MIN(avg_tier_price) OVER (ORDER BY lowest_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_avg_tier_price,
    SUM(avg_tier_price) OVER (ORDER BY lowest_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_avg_tier_price,
    AVG(avg_tier_price) OVER (ORDER BY lowest_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_avg_tier_price
FROM unpacked;
