WITH prepared_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        CONCAT_WS(' / ', category, sku_code) AS category_sku_code_label,
        ROUND(weight_kg / NULLIF(unit_cost, 0), 3) AS weight_kg_per_unit_cost
    FROM skus
    WHERE unit_cost >= 60.9
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(weight_kg_per_unit_cost, 3) OVER (PARTITION BY category ORDER BY weight_kg_per_unit_cost DESC NULLS FIRST, id) AS prev3_weight_kg_per_unit_cost,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER (PARTITION BY category), 0), 4) AS unit_cost_share_of_category
    FROM prepared_skus
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND TO_DATE(st.moved_at) < DATE '2025-02-24'
              AND st.operator IS NOT NULL
        )
)
SELECT
    id,
    sku_code,
    category,
    unit_cost,
    labels,
    category_sku_code_label,
    prev3_weight_kg_per_unit_cost,
    unit_cost_share_of_category,
    CASE WHEN category IN ('Apparel', 'Beauty') THEN 'group_a' WHEN category = 'Food' THEN 'group_b' ELSE 'other' END AS category_group,
    CASE WHEN unit_cost >= 320.4 THEN 'HIGH' WHEN unit_cost >= 164.5 THEN 'MEDIUM' ELSE 'LOW' END AS unit_cost_tier
FROM with_activity
ORDER BY category_sku_code_label DESC
