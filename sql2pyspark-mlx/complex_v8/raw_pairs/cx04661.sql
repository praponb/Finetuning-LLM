WITH filtered AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        ROUND(unit_cost / NULLIF(weight_kg, 0), 3) AS unit_cost_per_weight_kg,
        ROUND(unit_cost * 1.5, 2) AS unit_cost_adj,
        FLOOR(weight_kg / 5) * 5 AS weight_kg_bucket
    FROM skus
    WHERE category IN ('Food', 'Apparel')
),
latest_per_key AS (
    -- Keep the latest row per category
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        unit_cost_per_weight_kg,
        unit_cost_adj,
        weight_kg_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY unit_cost_adj DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM latest_per_key s
    WHERE s.id IN (
            SELECT st.sku_id
            FROM stock_moves st
            WHERE st.warehouse_id IS NOT NULL
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, unit_cost_adj, weight_kg, unit_cost
    FROM qualified
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (unit_cost_adj, weight_kg, unit_cost)
) unp
