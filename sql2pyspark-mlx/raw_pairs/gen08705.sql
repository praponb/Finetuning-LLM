WITH flattened_attrs AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        discounts,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
SELECT
    currency,
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT_IF(plan = 'starter') AS starter_events,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows
FROM flattened_attrs
GROUP BY ROLLUP (currency, DATE_TRUNC('QUARTER', effective_from))
HAVING COUNT(*) FILTER (WHERE currency = 'THB') > 1
ORDER BY currency DESC
