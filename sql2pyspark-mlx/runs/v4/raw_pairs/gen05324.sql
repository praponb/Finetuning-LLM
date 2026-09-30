WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
)
SELECT
    id,
    plan,
    currency,
    element,
    CASE WHEN currency RLIKE '^(EUR|THB)$' THEN 'PRIORITY' ELSE 'CORE' END AS currency_zone
FROM exploded;
