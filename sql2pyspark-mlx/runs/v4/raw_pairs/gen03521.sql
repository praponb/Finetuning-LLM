WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS tier_value
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM flattened
    WHERE effective_from > DATE '2024-06-15'
      AND tier_value <= 74
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(days_since_effective_from, 0) AS days_since_effective_from_filled
    FROM valid
    WHERE effective_from >= TO_DATE('2024-06-12')
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN tier_value >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_tier,
        DATE_TRUNC('YEAR', effective_from) AS year_start,
        COUNT(*) AS num_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(days_since_effective_from), 2) AS avg_days_since_effective_from,
        COUNT(DISTINCT id) AS distinct_ids_2,
        PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles,
        PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles_2
    FROM valid_price_tiers
    GROUP BY CASE WHEN tier_value >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', effective_from)
    HAVING COUNT(*) > 1
)
SELECT
    tier_value_tier,
    year_start,
    distinct_ids,
    avg_days_since_effective_from,
    distinct_ids_2,
    tier_value_quartiles
FROM summary
