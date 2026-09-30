WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS element
)
SELECT
    id,
    currency,
    CASE WHEN currency RLIKE '^(THB|EUR)$' THEN 'PRIORITY' WHEN currency RLIKE '^(USD)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS currency_zone
FROM flattened
ORDER BY id
