WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        discounts,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT(DISTINCT currency) AS distinct_currencies,
    APPROX_PERCENTILE(avg_tier_price, 0.5) AS avg_tier_price_p50,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows
FROM unpacked
GROUP BY DATE_TRUNC('MONTH', effective_from)
