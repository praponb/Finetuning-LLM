WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        tier_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN lowest_price >= 31 THEN 'LARGE' ELSE 'SMALL' END AS lowest_price_tier,
    COUNT(*) AS num_rows,
    COUNT_IF(plan = 'enterprise') AS enterprise_events
FROM parsed
GROUP BY lowest_price_tier
ORDER BY num_rows DESC NULLS LAST;
