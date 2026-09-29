WITH cleaned_skus AS (
    -- Filter skus
    SELECT
        id,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CONCAT(sku_code, '#', CAST(id AS STRING)) AS sku_code_ref,
        CONCAT_WS('|', category, sku_code) AS category_sku_code_label
    FROM skus
    WHERE NOT is_active
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM cleaned_skus
    WHERE weight_kg > (SELECT MIN(weight_kg) FROM cleaned_skus)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        weight_kg,
        labels,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_cost), 3) AS unit_cost_percent_rank,
        DENSE_RANK() OVER (PARTITION BY is_active ORDER BY unit_cost) AS unit_cost_dense_rank,
        COUNT(*) OVER (PARTITION BY is_active) AS rows_in_is_active,
        MIN(weight_kg) OVER (PARTITION BY is_active) AS is_active_min_weight_kg,
        FIRST_VALUE(weight_kg) OVER (PARTITION BY is_active ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_weight_kg,
        ROUND(PERCENT_RANK() OVER w_all_by_unit_cost, 3) AS unit_cost_percent_rank_2
    FROM screened
    WINDOW w_all_by_unit_cost AS (ORDER BY unit_cost, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(rows_in_is_active - 10) AS rows_in_is_active_distance,
        SIZE(labels) AS labels_count,
        CASE WHEN weight_kg >= 18.5 THEN 'large' WHEN weight_kg >= 6.5 THEN 'medium' ELSE 'small' END AS weight_kg_tier
    FROM windowed
),
leaders AS (
    -- First row per group
    SELECT
        id,
        weight_kg,
        labels,
        unit_cost_percent_rank,
        rows_in_is_active,
        is_active_min_weight_kg,
        first_weight_kg,
        unit_cost_percent_rank_2,
        rows_in_is_active_distance,
        labels_count,
        weight_kg_tier
    FROM derived
    WHERE unit_cost_dense_rank = 1
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        weight_kg,
        labels,
        unit_cost_percent_rank,
        is_active_min_weight_kg,
        first_weight_kg,
        unit_cost_percent_rank_2,
        rows_in_is_active_distance,
        labels_count,
        ROUND(is_active_min_weight_kg * 1.1, 2) AS is_active_min_weight_kg_adj,
        ROUND(unit_cost_percent_rank_2 * 0.25, 2) AS unit_cost_percent_rank_2_adj,
        GREATEST(first_weight_kg, labels_count) AS greatest_first_weight_kg_labels_count
    FROM leaders
    WHERE unit_cost_percent_rank < 0.6
)
SELECT
    id,
    weight_kg,
    labels,
    unit_cost_percent_rank,
    is_active_min_weight_kg,
    unit_cost_percent_rank_2,
    rows_in_is_active_distance,
    labels_count,
    unit_cost_percent_rank_2_adj,
    greatest_first_weight_kg_labels_count
FROM enriched
ORDER BY labels_count DESC, greatest_first_weight_kg_labels_count NULLS LAST
