WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS tier_value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(plan) AS plan_length,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM exploded
    WHERE currency NOT IN ('THB', 'USD')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        effective_from,
        currency,
        days_since_effective_from,
        days_since_effective_from / NULLIF(tier_value, 0) AS days_since_effective_from_per_tier_value,
        UPPER(plan) AS plan_upper
    FROM cleaned
    WHERE effective_from >= TO_DATE('2024-05-23')
),
scored AS (
    -- Running and moving aggregates
    SELECT *, DENSE_RANK() OVER (PARTITION BY currency ORDER BY days_since_effective_from NULLS LAST) AS days_since_effective_from_dense_rank
    FROM base
)
SELECT
    effective_from,
    currency,
    days_since_effective_from_dense_rank
FROM scored
ORDER BY effective_from NULLS LAST, days_since_effective_from_dense_rank DESC
