WITH unpacked AS (
    SELECT
        id,
        plan,
        effective_from,
        discounts,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
SELECT
    id,
    plan,
    effective_from,
    discounts,
    avg_tier_price,
    lowest_price,
    tier_revenue_caps,
    first_two_limits,
    net_prices,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY lowest_price DESC, id) event_seq,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY avg_tier_price) avg_tier_price_cume_dist,
    AVG(lowest_price) OVER (PARTITION BY plan ORDER BY lowest_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) lowest_price_avg_5
FROM unpacked
