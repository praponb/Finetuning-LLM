WITH unpacked AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
SELECT
    *,
    LAG(avg_tier_price) OVER (PARTITION BY plan ORDER BY entry_price, id) AS prev_avg_tier_price,
    LAG(entry_price) OVER (PARTITION BY plan ORDER BY entry_price, id) AS prev_entry_price,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY entry_price, id) AS seq_no
FROM unpacked
