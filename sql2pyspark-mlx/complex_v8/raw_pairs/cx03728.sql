WITH cleaned AS (
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
    WHERE ARRAY_CONTAINS(labels, 'hazmat')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(weight_kg, 2) OVER (PARTITION BY category, is_active ORDER BY unit_cost DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_weight_kg,
        LAST_VALUE(unit_cost) OVER (PARTITION BY category, is_active ORDER BY unit_cost DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_cost,
        COUNT(*) OVER (PARTITION BY category, is_active ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(weight_kg) IGNORE NULLS OVER (ORDER BY weight_kg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weight_kg,
        FIRST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY category, is_active ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_cost,
        ROUND(PERCENT_RANK() OVER w_all_by_weight_kg_ties, 3) AS weight_kg_percent_rank,
        LEAD(unit_cost, 2) OVER w_all_by_weight_kg AS next2_unit_cost,
        ROUND(AVG(weight_kg) OVER (PARTITION BY category, is_active ORDER BY unit_cost DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS weight_kg_avg_2_prior,
        RANK() OVER w_category_is_active_by_weight_2 AS weight_kg_rank
    FROM cleaned
    WINDOW w_all_by_weight_kg_ties AS (ORDER BY weight_kg DESC NULLS FIRST), w_all_by_weight_kg AS (ORDER BY weight_kg DESC NULLS FIRST, id), w_category_is_active_by_weight_2 AS (PARTITION BY category, is_active ORDER BY weight_kg, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        sku_code,
        weight_kg,
        labels,
        running_count,
        last_known_weight_kg,
        first_known_unit_cost,
        weight_kg_percent_rank,
        weight_kg_avg_2_prior,
        weight_kg_rank,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag,
        CONCAT_WS(': ', category, sku_code) AS category_sku_code_label
    FROM metrics
    WHERE next2_unit_cost BETWEEN 108.5 AND 358.5
)
-- Final projection
SELECT
    id,
    weight_kg,
    labels,
    running_count,
    weight_kg_percent_rank,
    weight_kg_avg_2_prior,
    weight_kg_rank,
    is_active_flag,
    category_sku_code_label
FROM derived
WHERE last_known_weight_kg >= 1.8
ORDER BY category_sku_code_label
