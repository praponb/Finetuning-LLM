WITH cleaned_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag,
        CASE WHEN weight_kg >= 14.6 THEN 'large' WHEN weight_kg >= 6.4 THEN 'medium' ELSE 'small' END AS weight_kg_tier
    FROM skus
    WHERE (UPPER(TRIM(sku_code)) LIKE 'E%' OR is_active = TRUE)
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_code,
        s.category,
        s.unit_cost,
        s.weight_kg,
        s.weight_kg_tier,
        st.id AS move_id,
        st.move_type,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price
    FROM cleaned_skus s
    LEFT JOIN stock_moves st ON st.sku_id = s.id
),
tagged AS (
    -- Add calculated columns
    SELECT *, LOWER(TRIM(move_type)) AS move_type_clean
    FROM exploded_detail_moves
)
SELECT
    id,
    sku_code,
    category,
    unit_cost,
    weight_kg,
    weight_kg_tier,
    move_id,
    move_type,
    move_moved_at,
    move_operator,
    move_unit_price,
    move_type_clean,
    weight_kg + unit_cost AS weight_kg_plus_unit_cost
FROM tagged
ORDER BY move_operator, sku_code DESC, id, move_id
LIMIT 10
