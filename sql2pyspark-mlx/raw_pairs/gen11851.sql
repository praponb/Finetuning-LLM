WITH parsed AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
SELECT
    CASE WHEN avg_tier_price >= 35 THEN 'LARGE' ELSE 'SMALL' END AS avg_tier_price_tier,
    MAX(avg_tier_price) AS max_avg_tier_price,
    ROUND(AVG(lowest_price), 2) AS avg_lowest_price,
    ROUND(SUM(lowest_price) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS lowest_price_per_starter
FROM parsed
GROUP BY CASE WHEN avg_tier_price >= 35 THEN 'LARGE' ELSE 'SMALL' END
