WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
),
base AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('YEAR', effective_from) AS effective_from_period
    FROM flattened
    WHERE effective_from < '2025-03-01'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT DISTINCT
    SELECT id
    FROM price_tiers
    WHERE currency IN ('USD', 'THB')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(element) OVER w_part AS element_max_30d,
        RANK() OVER w_part_2 AS element_rank,
        RANK() OVER w_part_2 AS element_rank_2,
        AVG(element) OVER (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS element_avg_90d
    FROM kept_rows
    WINDOW w_part AS (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_part_2 AS (ORDER BY element DESC NULLS LAST)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', effective_from_period) AS year_start,
    APPROX_PERCENTILE(element_rank_2, 0.9) AS element_rank_2_p90,
    APPROX_PERCENTILE(element, 0.5) AS element_p50,
    APPROX_PERCENTILE(element_avg_90d, ARRAY(0.25, 0.5, 0.75)) AS element_avg_90d_quartiles
FROM scored
GROUP BY year_start
ORDER BY year_start DESC
