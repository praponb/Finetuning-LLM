WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS(': ', move_type, operator) AS move_type_operator_label,
        CONCAT(move_type, '#', CAST(id AS STRING)) AS move_type_ref,
        CONCAT_WS('|', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 12:00:00'
),
deduped AS (
    -- Keep the earliest row per operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        move_type_operator_label,
        move_type_ref,
        operator_move_type_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY unit_price, id) AS row_num
        FROM recent_moves
    ) ranked
    WHERE row_num = 1
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.move_type_ref,
        s.operator_move_type_label,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.move_type,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty
    FROM deduped s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        MAX(unit_price) AS max_move_unit_price
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.max_move_unit_price
    FROM with_children s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(operator, 'ALL') AS operator,
    move_id,
    move_warehouse_id,
    GROUPING(operator) AS is_total_operator,
    MAX(move_moved_at) AS last_move_moved_at,
    COUNT(*) AS cnt,
    ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
FROM with_counts
GROUP BY GROUPING SETS ((operator, move_id, move_warehouse_id), (operator), (move_id))
ORDER BY operator NULLS LAST, move_id NULLS LAST, move_warehouse_id NULLS LAST
