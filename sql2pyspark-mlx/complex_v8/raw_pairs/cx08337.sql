WITH prepared AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        is_active,
        ROUND(unit_cost / NULLIF(weight_kg, 0), 3) AS unit_cost_per_weight_kg,
        CASE WHEN category IN ('Toys', 'Food') THEN 'group_a' WHEN category = 'Beauty' THEN 'group_b' ELSE 'other' END AS category_group
    FROM skus
    WHERE is_active = TRUE
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM prepared s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
latest_per_key AS (
    -- Keep the latest row per category
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        is_active,
        unit_cost_per_weight_kg,
        category_group,
        move_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY weight_kg DESC, id DESC) AS row_num
        FROM summarized_rows
    ) ranked
    WHERE row_num = 1
),
latest_per_key_2 AS (
    -- Keep the latest row per category_group, category
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        is_active,
        unit_cost_per_weight_kg,
        category_group,
        move_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category_group, category ORDER BY move_count DESC, id DESC) AS rn
        FROM latest_per_key
    ) ranked
    WHERE rn = 1
)
SELECT *
FROM (
    SELECT category_group, move_count, category, unit_cost_per_weight_kg
    FROM latest_per_key_2
) src
PIVOT (
    SUM(unit_cost_per_weight_kg)
    FOR category IN ('Food', 'Apparel')
)
ORDER BY category_group, move_count
