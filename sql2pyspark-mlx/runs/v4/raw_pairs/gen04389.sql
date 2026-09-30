WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_percent_rank_2
    FROM flattened
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(plan) AS plan_upper
    FROM enriched
    WHERE plan = 'enterprise'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM base
    EXCEPT
    SELECT id, plan
    FROM price_tiers
    WHERE plan = 'starter'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            currency,
            plan,
            val_percent_rank_2
        FROM verified
    ) src
    PIVOT (
        SUM(val_percent_rank_2) AS total,
        COUNT(1) AS cnt
        FOR plan IN ('pro' AS pro, 'starter' AS starter)
    )
)
SELECT
    currency,
    pro_total,
    starter_total,
    CASE WHEN currency RLIKE '^(USD|THB)$' THEN 'PRIORITY' WHEN currency RLIKE '^(EUR)$' THEN 'EXTENDED' ELSE 'CORE' END AS currency_zone
FROM pivoted;
