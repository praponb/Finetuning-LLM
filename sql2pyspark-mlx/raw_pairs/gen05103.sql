WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS element
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        currency,
        element,
        ROUND(element, 0) AS element_rounded
    FROM lines
    WHERE effective_from > DATE '2024-05-11'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        element,
        element_rounded,
        PERCENT_RANK() OVER (PARTITION BY plan, currency ORDER BY element_rounded NULLS FIRST) AS element_rounded_percent_rank
    FROM base
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY element_rounded_percent_rank DESC NULLS FIRST) AS element_rounded_percent_rank_percent_rank,
        RANK() OVER (ORDER BY element_rounded DESC) AS element_rounded_rank
    FROM scored
    WHERE element_rounded_percent_rank BETWEEN 0.4 AND 0.56
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        element_rounded_percent_rank_percent_rank,
        element_rounded_rank,
        ROUND(element_rounded_rank, 1) AS element_rounded_rank_rounded
    FROM metrics
    WHERE element < 63
)
SELECT element_rounded_rank, element_rounded_rank_rounded
FROM filtered
ORDER BY element_rounded_rank NULLS FIRST, element_rounded_rank_rounded;
