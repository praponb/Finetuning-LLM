WITH prepared AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        is_active,
        CASE WHEN unit_cost >= 306.4 THEN 'HIGH' WHEN unit_cost >= 68.0 THEN 'MEDIUM' ELSE 'LOW' END AS unit_cost_tier,
        CASE WHEN category IN ('Food', 'Apparel') THEN 'group_a' WHEN category = 'Electronics' THEN 'group_b' ELSE 'other' END AS category_group
    FROM skus
    WHERE unit_cost >= 164.5
      AND weight_kg < 14.3
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.qty >= 168
              AND st.operator = 'op_ann'
        )
),
flattened AS (
    -- One row per label
    SELECT
        s.id,
        s.sku_code,
        s.category,
        s.weight_kg,
        s.is_active,
        s.unit_cost_tier,
        s.category_group,
        t.label
    FROM qualified s
    LATERAL VIEW EXPLODE(s.labels) t AS label
),
deduped AS (
    -- Keep the latest row per category_group
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        is_active,
        unit_cost_tier,
        category_group,
        label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category_group ORDER BY weight_kg DESC, id DESC, label DESC) AS rn
        FROM flattened
    ) ranked
    WHERE rn = 1
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_code,
        category,
        is_active,
        category_group,
        label,
        NTH_VALUE(weight_kg, 2) OVER (PARTITION BY is_active, category_group ORDER BY weight_kg, id, label ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_weight_kg,
        SUM(weight_kg) OVER (PARTITION BY is_active, category_group ORDER BY weight_kg, id, label ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weight_kg_sum_7
    FROM deduped
)
-- Final projection
SELECT
    id,
    sku_code,
    category,
    is_active,
    category_group,
    label,
    COALESCE(second_weight_kg, 0.0) AS second_weight_kg_filled,
    weight_kg_sum_7 - second_weight_kg AS weight_kg_sum_7_minus_second_weight_kg
FROM sequenced
WHERE category_group IN ('other', 'group_b')
ORDER BY id, label
LIMIT 20
