WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS val
),
filtered AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        CONCAT_WS('/', currency, plan) AS currency_plan_key
    FROM exploded
    WHERE val >= 78
),
cleaned AS (
    SELECT
        id,
        effective_from,
        currency,
        val,
        currency_plan_key,
        DATE_ADD(effective_from, 67) AS effective_from_plus_67d,
        WEEKOFYEAR(effective_from) AS effective_from_weekofyear
    FROM filtered
    WHERE currency <> 'THB'
),
valid AS (
    SELECT *, CAST(val AS STRING) AS val_num
    FROM cleaned
    WHERE MONTH(effective_from) IN (3, 10, 12)
)
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    currency,
    COUNT_IF(currency = 'THB') AS thb_events,
    COUNT(DISTINCT id) AS distinct_ids
FROM valid
GROUP BY DATE_TRUNC('YEAR', effective_from), currency
ORDER BY year_start
