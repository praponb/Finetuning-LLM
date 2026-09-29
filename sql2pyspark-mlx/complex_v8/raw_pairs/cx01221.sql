WITH base_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        LEAST(unit_cost, weight_kg) AS least_unit_cost_weight_kg,
        CONCAT_WS(': ', category, sku_code) AS category_sku_code_label
    FROM skus
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_code,
        unit_cost,
        least_unit_cost_weight_kg,
        category_sku_code_label,
        ROW_NUMBER() OVER w_is_active_by_weight_kg AS weight_kg_seq,
        LAG(least_unit_cost_weight_kg, 2) OVER w_is_active_by_weight_kg AS prev2_least_unit_cost_weight_kg,
        unit_cost - LAG(unit_cost) OVER (PARTITION BY is_active ORDER BY weight_kg DESC, id) AS unit_cost_change,
        RANK() OVER w_is_active_by_weight_kg_ties AS weight_kg_rank
    FROM base_skus
    WINDOW w_is_active_by_weight_kg AS (PARTITION BY is_active ORDER BY weight_kg, id), w_is_active_by_weight_kg_ties AS (PARTITION BY is_active ORDER BY weight_kg DESC)
)
SELECT id, metric_name, value
FROM (
    SELECT id, weight_kg_seq, weight_kg_rank
    FROM with_trends
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (weight_kg_seq, weight_kg_rank)
) unp
