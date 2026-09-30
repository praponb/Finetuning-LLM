WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(element, 0) AS element_rounded
    FROM exploded
    WHERE YEAR(effective_from) >= 2023
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(element_rounded * 1.1, 2) AS element_rounded_adjusted,
        element_rounded + element AS element_rounded_plus_element
    FROM valid
    WHERE effective_from BETWEEN '2023-08-01' AND '2025-07-27'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE MONTH(effective_from) IN (5, 6, 8)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(element_rounded_plus_element, ARRAY(0.25, 0.5, 0.75)) AS element_rounded_plus_element_quartiles,
    MIN(element) AS min_element
FROM kept_rows
GROUP BY CUBE (DATE_TRUNC('WEEK', effective_from))
HAVING min_element > 0;
