WITH parsed AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE currency = 'EUR'
)
SELECT
    CASE WHEN entry_price >= 30 THEN 'PREMIUM' ELSE 'STANDARD' END AS entry_price_tier,
    APPROX_PERCENTILE(avg_tier_price, 0.9) AS avg_tier_price_p90,
    COUNT(*) AS cnt
FROM parsed
GROUP BY CUBE (CASE WHEN entry_price >= 30 THEN 'PREMIUM' ELSE 'STANDARD' END);
