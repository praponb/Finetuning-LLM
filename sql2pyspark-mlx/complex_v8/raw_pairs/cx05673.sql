WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
        COALESCE(qty, 0) AS qty_filled,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.moved_at_label,
        s.qty_filled,
        st.id AS move_id,
        st.move_type,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price
    FROM recent_moves s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
latest AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        moved_at_label,
        qty_filled,
        move_id,
        move_type,
        move_moved_at,
        move_unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty_filled DESC, id DESC, move_id DESC) AS row_num
        FROM line_level
    ) ranked
    WHERE row_num = 1
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.moved_at_label,
        s.qty_filled,
        s.move_id,
        s.move_type,
        s.move_unit_price,
        st.id AS move_id_2,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at_2,
        st.move_type AS move_move_type,
        st.warehouse_id AS move_warehouse_id
    FROM latest s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
deduped AS (
    -- Keep the latest row per move_operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        moved_at_label,
        qty_filled,
        move_id,
        move_type,
        move_unit_price,
        move_id_2,
        move_operator,
        move_moved_at_2,
        move_move_type,
        move_warehouse_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_operator, sku_id ORDER BY qty DESC, id DESC, move_id DESC, move_id_2 DESC) AS row_num
        FROM detail_rows
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        moved_at_label,
        qty_filled,
        move_id,
        move_type,
        move_unit_price,
        move_id_2,
        move_operator,
        move_moved_at_2,
        move_move_type,
        move_warehouse_id,
        ROUND(move_unit_price - AVG(move_unit_price) OVER (PARTITION BY move_id), 2) AS move_unit_price_vs_move_id_avg,
        SIZE(COLLECT_SET(move_move_type) OVER (PARTITION BY move_id)) AS distinct_move_move_type_in_move_id,
        SIZE(COLLECT_SET(move_operator) OVER (PARTITION BY move_id)) AS distinct_move_operator_in_move_id,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_id)) AS distinct_sku_id_in_move_id,
        NTH_VALUE(move_unit_price, 2) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id, move_id, move_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_move_unit_price,
        SUM(move_unit_price) OVER (PARTITION BY move_id) AS move_id_sum_move_unit_price
    FROM deduped
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(move_move_type)) AS move_move_type_len,
        INITCAP(TRIM(move_operator)) AS move_operator_clean
    FROM with_trends
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_moved_at_2, move_move_type, move_id_sum_move_unit_price
    FROM labeled
) src
PIVOT (
    MAX(move_id_sum_move_unit_price)
    FOR move_move_type IN ('ADJUST', 'INBOUND')
)
ORDER BY move_moved_at_2
