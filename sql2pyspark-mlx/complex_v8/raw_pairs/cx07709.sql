WITH valid_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        ROUND(unit_cost / NULLIF(weight_kg, 0), 3) AS unit_cost_per_weight_kg
    FROM skus
    WHERE sku_code IS NOT NULL
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM valid_skus s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.warehouse_id IS NOT NULL
              AND st.unit_price < 290.4
        )
      AND s.weight_kg BETWEEN 8.5 AND 20.9
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown,
        CONCAT(sku_code, '#', CAST(id AS STRING)) AS sku_code_ref,
        GREATEST(unit_cost_per_weight_kg, unit_cost) AS greatest_unit_cost_per_weight_kg_unit_cost
    FROM with_activity
    WHERE category IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(weight_kg) OVER (PARTITION BY category, category_or_unknown) AS category_category_or_unknown_min_weight_kg,
        ROUND(weight_kg / NULLIF(SUM(weight_kg) OVER (PARTITION BY category, category_or_unknown), 0), 4) AS weight_kg_share_of_category_category_or_unknown,
        NTILE(5) OVER (PARTITION BY category, category_or_unknown ORDER BY unit_cost DESC, id) AS unit_cost_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY category, category_or_unknown ORDER BY unit_cost DESC), 3) AS unit_cost_cume_dist
    FROM tagged
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('|', sku_code_ref, sku_code) AS sku_code_ref_sku_code_label
    FROM metrics
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(unit_cost_per_weight_kg) OVER (PARTITION BY category_or_unknown, unit_cost_quintile), 2) AS category_or_unknown_unit_cost_quintile_avg_unit_,
        LEAD(weight_kg) OVER w_all_by_unit_cost_per_weight AS next_weight_kg,
        COUNT(*) OVER (PARTITION BY category_or_unknown, unit_cost_quintile) AS rows_in_category_or_unknown_unit_cost_quintile,
        ROUND(AVG(unit_cost_quintile) OVER (PARTITION BY category_or_unknown, unit_cost_quintile), 2) AS category_or_unknown_unit_cost_quintile_avg_unit__2,
        SUM(unit_cost_cume_dist) OVER (ORDER BY unit_cost_per_weight_kg DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unit_cost_cume_dist_sum_2,
        RANK() OVER w_unit_cost_quintile_category AS weight_kg_share_of_category_category_or_unknown_
    FROM labeled
    WINDOW w_all_by_unit_cost_per_weight AS (ORDER BY unit_cost_per_weight_kg DESC, id), w_unit_cost_quintile_category AS (PARTITION BY unit_cost_quintile, category ORDER BY weight_kg_share_of_category_category_or_unknown NULLS LAST)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, next_weight_kg, category_category_or_unknown_min_weight_kg, greatest_unit_cost_per_weight_kg_unit_cost
    FROM ranked
) src
UNPIVOT (
    metric_value FOR metric IN (next_weight_kg, category_category_or_unknown_min_weight_kg, greatest_unit_cost_per_weight_kg_unit_cost)
) unp
ORDER BY id, metric
