WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        tier_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN lowest_price >= 27 THEN 'LARGE' ELSE 'SMALL' END AS lowest_price_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(plan = 'enterprise') AS enterprise_events,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(*) AS cnt,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows,
    GROUPING_ID() AS grouping_id
FROM parsed
GROUP BY ROLLUP (CASE WHEN lowest_price >= 27 THEN 'LARGE' ELSE 'SMALL' END);
