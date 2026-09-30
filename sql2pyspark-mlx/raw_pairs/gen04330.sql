WITH flattened_attrs AS (
    SELECT
        id,
        currency,
        tier_limits,
        discounts,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
SELECT
    CASE WHEN entry_price >= 33 THEN 'HIGH' ELSE 'LOW' END AS entry_price_tier,
    currency,
    SUM(CASE WHEN currency = 'THB' THEN entry_price ELSE 0 END) AS thb_entry_price,
    AVG(avg_tier_price) FILTER (WHERE currency = 'EUR') AS eur_avg_avg_tier_price,
    SIZE(COLLECT_SET(currency)) AS num_currencies
FROM flattened_attrs
GROUP BY 1, 2
ORDER BY entry_price_tier DESC NULLS LAST;
