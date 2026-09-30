WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS val
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN plan = 'basic' THEN 1 ELSE 0 END AS is_basic
    FROM flattened
    WHERE val BETWEEN 47 AND 54
),
valid_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        val,
        is_basic,
        CEIL(is_basic) AS is_basic_whole,
        CEIL(val) AS val_whole
    FROM valid
    WHERE YEAR(effective_from) = 2023
)
SELECT
    plan,
    val,
    is_basic,
    is_basic_whole,
    val_whole
FROM valid_price_tiers
ORDER BY is_basic DESC NULLS FIRST, val
