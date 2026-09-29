WITH unpacked AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        discounts,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
    WHERE plan = 'basic'
)
SELECT
    id,
    plan,
    LAG(avg_tier_price) OVER (PARTITION BY plan ORDER BY effective_from, id) prev_avg_tier_price,
    LEAD(avg_tier_price) OVER (PARTITION BY plan ORDER BY effective_from, id) next_avg_tier_price,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY effective_from, id) event_seq,
    NTILE(3) OVER (PARTITION BY plan ORDER BY avg_tier_price DESC, id) avg_tier_price_bucket
FROM unpacked;
