WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE move_type = 'ADJUST'
      AND warehouse_id <= 5
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.unit_cost AS sku_unit_cost
    FROM recent_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.sku_unit_cost,
        st.id AS move_id,
        st.qty AS move_qty,
        st.move_type AS move_move_type,
        st.moved_at AS move_moved_at
    FROM joined_sku s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(move_move_type)) AS move_move_type_len,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM line_level
)
-- Final projection
SELECT
    id,
    sku_id,
    operator,
    sku_unit_cost,
    move_qty,
    move_move_type,
    move_moved_at,
    move_move_type_len,
    operator_or_unknown,
    ABS(sku_unit_cost - 251.2) AS sku_unit_cost_distance,
    FLOOR(move_move_type_len / 2) * 2 AS move_move_type_len_bucket
FROM labeled
ORDER BY sku_unit_cost_distance NULLS LAST, sku_id NULLS LAST
