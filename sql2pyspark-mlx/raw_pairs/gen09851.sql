WITH parsed AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
SELECT
    currency,
    CASE WHEN avg_tier_price >= 25 THEN 'HIGH' ELSE 'LOW' END AS avg_tier_price_tier,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
    ROUND(SUM(avg_tier_price) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS avg_tier_price_per_starter,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows
FROM parsed
GROUP BY currency, avg_tier_price_tier
HAVING ROUND(SUM(avg_tier_price) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) > 0
ORDER BY eur_count NULLS LAST;
