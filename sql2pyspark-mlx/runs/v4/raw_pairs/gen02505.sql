WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        currency,
        element,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) element_dense_rank,
        CUME_DIST() OVER w_grp element_cume_dist
    FROM exploded
    WINDOW w_grp AS (PARTITION BY currency ORDER BY element DESC NULLS FIRST)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(currency, 1, 2) currency_prefix,
        DATEDIFF(CURRENT_DATE(), effective_from) days_since_effective_from
    FROM metrics
    WHERE element BETWEEN 36 AND 57
)
SELECT
    effective_from,
    currency,
    element_cume_dist,
    currency_prefix,
    days_since_effective_from
FROM cleaned
