WITH scoped AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE (is_active = FALSE OR LOWER(sku_code) LIKE '%08%')
),
category_stats AS (
    -- Median unit_cost per category
    SELECT category, PERCENTILE_APPROX(unit_cost, 0.5) AS median_unit_cost
    FROM scoped
    GROUP BY category
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_cost,
        ROUND(s.unit_cost - g.median_unit_cost, 2) AS unit_cost_minus_median
    FROM scoped s
    JOIN category_stats g ON g.category = s.category
    WHERE s.unit_cost >= g.median_unit_cost
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        median_unit_cost - unit_cost AS median_unit_cost_minus_unit_cost,
        IF(is_active, 'yes', 'no') AS is_active_label,
        ARRAY_CONTAINS(labels, 'cold') AS has_cold
    FROM vs_group
),
unique_rows AS (
    -- Keep the latest row per is_active_label
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        median_unit_cost,
        unit_cost_minus_median,
        median_unit_cost_minus_unit_cost,
        is_active_label,
        has_cold
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY is_active_label ORDER BY unit_cost DESC, id DESC) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        has_cold,
        is_active_label,
        MAX(median_unit_cost) AS max_median_unit_cost,
        ROUND(SUM(weight_kg), 2) AS total_weight_kg
    FROM unique_rows
    GROUP BY has_cold, is_active_label
    HAVING MAX(unit_cost_minus_median) > -120.0
),
labeled AS (
    -- Label and score rows
    SELECT *, COALESCE(max_median_unit_cost, 0.0) AS max_median_unit_cost_filled
    FROM rollup_base
)
SELECT
    has_cold,
    is_active_label,
    max_median_unit_cost,
    max_median_unit_cost_filled,
    CASE WHEN max_median_unit_cost >= 300.2 THEN 'large' WHEN max_median_unit_cost >= 100.8 THEN 'medium' ELSE 'small' END AS max_median_unit_cost_tier,
    COALESCE(max_median_unit_cost, 0.0) AS max_median_unit_cost_filled_2
FROM labeled
ORDER BY max_median_unit_cost, is_active_label
