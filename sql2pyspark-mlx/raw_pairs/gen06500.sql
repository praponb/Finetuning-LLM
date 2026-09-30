WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(element, 2) element_rounded,
        ADD_MONTHS(effective_from, 6) effective_from_plus_months
    FROM lines
    WHERE effective_from >= DATE '2024-02-09'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        element,
        element_rounded,
        RANK() OVER (ORDER BY element_rounded DESC NULLS FIRST) element_rounded_rank
    FROM filtered
    WHERE effective_from_plus_months BETWEEN '2024-07-30' AND '2025-10-23'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, CUME_DIST() OVER (ORDER BY element_rounded DESC NULLS LAST) element_rounded_cume_dist
    FROM enriched
)
SELECT
    id,
    effective_from,
    element,
    element_rounded,
    element_rounded_rank,
    element_rounded_cume_dist
FROM scored
ORDER BY element DESC, element_rounded;
