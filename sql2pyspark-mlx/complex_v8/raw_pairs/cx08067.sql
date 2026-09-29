WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE qty < 314
      AND operator = 'op_ann'
),
unique_rows AS (
    -- Keep the latest row per operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        greatest_qty_unit_price,
        operator_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY moved_at DESC, id DESC) AS recency_rank
        FROM cleaned_moves
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(greatest_qty_unit_price / 100) * 100 AS greatest_qty_unit_price_bucket,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown_2,
        ABS(greatest_qty_unit_price - 250.5) AS greatest_qty_unit_price_distance
    FROM unique_rows
    WHERE unit_price >= 180.0
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.greatest_qty_unit_price,
        s.greatest_qty_unit_price_distance,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty,
        st.warehouse_id AS move_warehouse_id
    FROM tagged s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Subtotals with CUBE
SELECT
    move_type,
    move_id,
    operator,
    SUM(move_qty) AS total_move_qty,
    ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
FROM line_level
GROUP BY CUBE(move_type, move_id, operator)
ORDER BY move_type NULLS LAST, move_id NULLS LAST, operator NULLS LAST
