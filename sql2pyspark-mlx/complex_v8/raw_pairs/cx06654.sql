WITH cleaned AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug,
        unit_cost + weight_kg AS unit_cost_plus_weight_kg,
        COALESCE(weight_kg, 0.0) AS weight_kg_filled
    FROM skus
    WHERE UPPER(TRIM(sku_code)) LIKE 'A%'
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.warehouse_id IS NOT NULL
        )
      AND LENGTH(TRIM(s.sku_code)) >= 6
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        category_slug,
        unit_cost_plus_weight_kg,
        weight_kg_filled,
        LAST_VALUE(unit_cost_plus_weight_kg) IGNORE NULLS OVER (ORDER BY unit_cost_plus_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost_plus_weight_kg,
        unit_cost_plus_weight_kg - LAG(unit_cost_plus_weight_kg) OVER w_all_by_unit_cost_plus_weight_2 AS unit_cost_plus_weight_kg_change,
        NTILE(3) OVER (ORDER BY weight_kg_filled DESC, id) AS weight_kg_filled_tercile,
        LAG(unit_cost_plus_weight_kg, 2) OVER (ORDER BY unit_cost, id) AS prev2_unit_cost_plus_weight_kg
    FROM screened
    WINDOW w_all_by_unit_cost_plus_weight_2 AS (ORDER BY unit_cost_plus_weight_kg, id)
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM metrics s
    WHERE s.id IN (
            SELECT st.sku_id
            FROM stock_moves st
        )
)
SELECT
    id,
    sku_code,
    unit_cost,
    category_slug,
    unit_cost_plus_weight_kg,
    weight_kg_filled,
    unit_cost_plus_weight_kg_change,
    weight_kg_filled_tercile,
    prev2_unit_cost_plus_weight_kg,
    LENGTH(TRIM(category_slug)) AS category_slug_len,
    CASE WHEN weight_kg_filled >= 17.5 THEN 'large' WHEN weight_kg_filled >= 7.6 THEN 'medium' ELSE 'small' END AS weight_kg_filled_tier
FROM qualified
ORDER BY weight_kg_filled
