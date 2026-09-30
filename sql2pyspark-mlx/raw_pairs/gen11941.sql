WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        effective_from,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
-- Ranking and gap features
SELECT
    id,
    lowest_price,
    avg_tier_price,
    SUM(avg_tier_price) OVER w_ordered AS last7_sum_avg_tier_price,
    MIN(avg_tier_price) OVER w_ordered AS last7_min_avg_tier_price,
    MAX(avg_tier_price) OVER w_ordered AS last7_max_avg_tier_price,
    COUNT(avg_tier_price) OVER w_ordered AS last7_count_avg_tier_price
FROM unpacked
WINDOW w_ordered AS (ORDER BY effective_from, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
