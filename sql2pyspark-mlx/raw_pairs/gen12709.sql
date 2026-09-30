WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS element
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        effective_from,
        element,
        CONCAT(currency, ', ', plan) AS currency_plan
    FROM flattened
    WHERE currency IN ('EUR', 'USD')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        effective_from,
        element,
        currency_plan,
        PERCENT_RANK() OVER (ORDER BY element NULLS LAST) AS element_percent_rank
    FROM base
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(element, 0) AS element_filled,
        element_percent_rank + element AS element_percent_rank_plus_element
    FROM with_windows
    WHERE element_percent_rank >= 0.56
),
metrics AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        currency_plan,
        element_filled,
        PERCENT_RANK() OVER (ORDER BY element_filled NULLS FIRST) AS element_filled_percent_rank
    FROM cleaned
),
scored AS (
    -- Ranking and gap features
    SELECT
        currency_plan,
        element_filled,
        PERCENT_RANK() OVER (ORDER BY element_filled_percent_rank DESC NULLS FIRST) AS element_filled_percent_rank_percent_rank,
        DENSE_RANK() OVER (ORDER BY element_filled_percent_rank DESC) AS element_filled_percent_rank_dense_rank
    FROM metrics
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, element_filled_percent_rank_dense_rank + element_filled_percent_rank_percent_rank AS element_filled_percent_rank_dense_rank_plus_element_filled_percent_rank_percent_rank
    FROM scored
    WHERE element_filled BETWEEN 51 AND 67
)
-- Grouped metrics
SELECT
    CASE WHEN element_filled_percent_rank_percent_rank >= 0.86 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_filled_percent_rank_percent_rank_tier,
    MAX(element_filled) AS max_element_filled,
    PERCENTILE_APPROX(element_filled, ARRAY(0.25, 0.5, 0.75)) AS element_filled_quartiles,
    COUNT(*) AS row_count
FROM filtered
GROUP BY 1
