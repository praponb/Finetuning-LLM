WITH parsed AS (
    SELECT
        id,
        plan,
        currency,
        tier_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
SELECT
    CASE WHEN avg_tier_price >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END AS avg_tier_price_tier,
    COUNT(DISTINCT plan) AS distinct_plans,
    PERCENTILE_APPROX(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles,
    COUNT_IF(plan = 'enterprise') AS enterprise_events,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT_IF(currency = 'EUR') AS eur_events
FROM parsed
GROUP BY CUBE (CASE WHEN avg_tier_price >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY avg_tier_price_tier ASC NULLS LAST
