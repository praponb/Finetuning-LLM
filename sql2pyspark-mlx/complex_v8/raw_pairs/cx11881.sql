WITH valid AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CASE WHEN unit_cost >= 277.0 THEN 'A' WHEN unit_cost >= 108.5 THEN 'B' ELSE 'C' END AS unit_cost_tier,
        ARRAY_CONTAINS(labels, 'bulky') AS has_bulky
    FROM skus
    WHERE (NOT is_active OR weight_kg > 6.5)
),
unique_rows AS (
    -- Keep the latest row per category, unit_cost_tier
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        unit_cost_tier,
        has_bulky
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category, unit_cost_tier ORDER BY unit_cost DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN unit_cost >= 239.3 THEN 'large' WHEN unit_cost >= 158.7 THEN 'medium' ELSE 'small' END AS unit_cost_tier_2,
        LENGTH(TRIM(unit_cost_tier)) AS unit_cost_tier_len,
        CONCAT(sku_code, '#', CAST(id AS STRING)) AS sku_code_ref
    FROM unique_rows
    WHERE is_active = TRUE
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM scored
    WHERE unit_cost < (SELECT MAX(unit_cost) FROM scored)
)
-- Subtotals with GROUPING SETS
SELECT
    category,
    unit_cost_tier_2,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT category) AS distinct_categorys,
    SUM(weight_kg) AS total_weight_kg
FROM filtered
GROUP BY GROUPING SETS ((category, unit_cost_tier_2), (category), ())
ORDER BY category NULLS LAST, unit_cost_tier_2 NULLS LAST
