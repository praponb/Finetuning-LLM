WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS val
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(currency) AS currency_lower,
        CASE WHEN val >= 71 THEN 'High' WHEN val >= 20 THEN 'Medium' ELSE 'Low' END AS val_level
    FROM lines
    WHERE effective_from > TO_DATE('2024-06-23')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_rank,
        RANK() OVER (PARTITION BY currency ORDER BY val) AS val_rank_2,
        RANK() OVER (PARTITION BY currency, plan ORDER BY val NULLS LAST) AS val_rank_3
    FROM valid
),
remaining_keys AS (
    -- Set difference on keys
    SELECT currency
    FROM with_windows
    EXCEPT
    SELECT currency
    FROM price_tiers
    WHERE effective_from > '2023-11-22'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN remaining_keys k ON s.currency = k.currency
)
SELECT
    id,
    plan,
    val,
    currency_lower,
    val_rank,
    CASE WHEN plan RLIKE '^(enterprise|starter)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS plan_zone
FROM verified
ORDER BY id NULLS LAST, val;
