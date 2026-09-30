WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE(tier_prices) AS element
    FROM price_tiers
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        element,
        DENSE_RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_dense_rank
    FROM lines
    WHERE element <= 31
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(element) AS element_whole
    FROM with_windows
    WHERE element BETWEEN 30 AND 78
),
base_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(element, element_dense_rank) AS least_element_element_dense_rank
    FROM base
    WHERE element_whole > 37
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        element,
        element_dense_rank,
        element_whole,
        CAST(element_whole AS STRING) AS element_whole_str,
        element_dense_rank / NULLIF(element_whole, 0) AS element_dense_rank_per_element_whole
    FROM base_price_tiers
    WHERE element >= 71
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, element - element_dense_rank_per_element_whole AS element_minus_element_dense_rank_per_element_whole
    FROM filtered
    WHERE element < 70
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN element >= 70 THEN 'HIGH' ELSE 'LOW' END AS element_tier,
        PERCENTILE_APPROX(element_minus_element_dense_rank_per_element_whole, 0.9) AS element_minus_element_dense_rank_per_element_whole_p90,
        COUNT(*) AS num_rows,
        PERCENTILE_APPROX(element_dense_rank_per_element_whole, 0.5) AS element_dense_rank_per_element_whole_p50
    FROM valid
    GROUP BY CASE WHEN element >= 70 THEN 'HIGH' ELSE 'LOW' END
    HAVING element_minus_element_dense_rank_per_element_whole_p90 > 0
)
SELECT
    element_tier,
    element_minus_element_dense_rank_per_element_whole_p90,
    num_rows,
    element_dense_rank_per_element_whole_p50
FROM grouped
ORDER BY element_dense_rank_per_element_whole_p50 DESC
