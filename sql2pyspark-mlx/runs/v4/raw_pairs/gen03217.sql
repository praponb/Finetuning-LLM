WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS val
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('/', plan, currency) AS plan_currency_key
    FROM flattened
    WHERE currency IN ('THB', 'EUR')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN val >= 68 THEN 'High' WHEN val >= 38 THEN 'Medium' ELSE 'Low' END AS val_level
    FROM cleaned
    WHERE currency != 'EUR'
)
SELECT
    plan,
    currency,
    val,
    val_level,
    CASE WHEN currency RLIKE '^(THB|EUR)$' THEN 'GROUP_A' WHEN currency RLIKE '^(USD)$' THEN 'CORE' ELSE 'EXTENDED' END AS currency_zone
FROM base
ORDER BY currency_zone DESC NULLS LAST;
