WITH unpacked AS (
    SELECT
        id,
        plan,
        effective_from,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
    WHERE plan <> 'basic'
)
SELECT
    id,
    plan,
    effective_from,
    entry_price,
    lowest_price,
    tier_revenue_caps,
    first_two_limits,
    net_prices,
    SUM(lowest_price) OVER w_recent AS cumulative_lowest_price,
    LAST_VALUE(entry_price) IGNORE NULLS OVER w_recent AS last_known_entry_price,
    MIN(entry_price) OVER (PARTITION BY plan) AS plan_min_entry_price,
    NTILE(10) OVER (PARTITION BY plan ORDER BY entry_price, id) AS entry_price_bucket
FROM unpacked
WINDOW w_recent AS (PARTITION BY plan ORDER BY lowest_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
