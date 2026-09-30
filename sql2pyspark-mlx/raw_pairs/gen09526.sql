WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS tier_value
)
-- Aggregate with conditional metrics
SELECT
    currency,
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    COUNT(DISTINCT currency) AS distinct_currencies,
    SUM(tier_value) AS total_tier_value,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    SUM(CASE WHEN currency = 'USD' THEN tier_value ELSE 0 END) AS usd_tier_value,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    COUNT_IF(currency = 'EUR') AS eur_events
FROM flattened
GROUP BY currency, week_start;
