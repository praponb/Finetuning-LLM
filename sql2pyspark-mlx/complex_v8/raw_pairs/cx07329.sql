WITH scoped AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        IF(is_active, 'yes', 'no') AS is_active_label
    FROM skus
    WHERE unit_cost >= 108.5
      AND SIZE(labels) >= 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(unit_cost) OVER (PARTITION BY is_active, is_active_label ORDER BY weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_cost,
        ROW_NUMBER() OVER (PARTITION BY is_active ORDER BY weight_kg DESC, id) AS weight_kg_seq,
        LEAD(unit_cost, 3, 0) OVER (PARTITION BY is_active, category ORDER BY weight_kg, id) AS next3_unit_cost,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER (PARTITION BY category), 0), 4) AS unit_cost_share_of_category,
        ROUND(unit_cost - AVG(unit_cost) OVER (PARTITION BY category, is_active), 2) AS unit_cost_vs_category_is_active_avg,
        COUNT(*) OVER (PARTITION BY category) AS rows_in_category,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER (PARTITION BY category, is_active), 0), 4) AS unit_cost_share_of_category_is_active,
        SUM(weight_kg) OVER (PARTITION BY category ORDER BY unit_cost DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS weight_kg_sum_4,
        MAX(weight_kg) OVER (PARTITION BY is_active, category ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_weight_kg_so_far,
        ROUND(weight_kg / NULLIF(SUM(weight_kg) OVER (PARTITION BY category, is_active), 0), 4) AS weight_kg_share_of_category_is_active,
        ROW_NUMBER() OVER w_category_by_unit_cost AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_active, category ORDER BY weight_kg, id), 3) AS weight_kg_percent_rank,
        SUM(weight_kg) OVER (PARTITION BY category ORDER BY unit_cost DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS weight_kg_sum_2
    FROM scoped
    WINDOW w_category_by_unit_cost AS (PARTITION BY category ORDER BY unit_cost DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT *, SIZE(labels) AS labels_count, LENGTH(TRIM(sku_code)) AS sku_code_len
    FROM with_trends
)
SELECT
    id,
    sku_code,
    category,
    labels,
    is_active,
    is_active_label,
    first_unit_cost,
    weight_kg_seq,
    next3_unit_cost,
    unit_cost_share_of_category,
    unit_cost_vs_category_is_active_avg,
    unit_cost_share_of_category_is_active,
    weight_kg_sum_4,
    max_weight_kg_so_far,
    weight_kg_share_of_category_is_active,
    seq_no,
    weight_kg_percent_rank,
    weight_kg_sum_2,
    labels_count,
    sku_code_len
FROM labeled
ORDER BY unit_cost_share_of_category_is_active DESC, id
LIMIT 5
