WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 10 AND 16
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.operator_slug,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty,
        st.operator AS move_operator
    FROM with_activity s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.operator NOT IN ('op_dew', 'op_eve')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(unit_price) OVER (ORDER BY move_qty NULLS LAST, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        COUNT(*) OVER (PARTITION BY move_type, move_id) AS rows_in_move_type_move_id,
        DENSE_RANK() OVER (ORDER BY move_qty NULLS LAST, id, move_id) AS move_qty_dense_rank,
        ROW_NUMBER() OVER (ORDER BY move_qty NULLS LAST, id, move_id) AS move_qty_seq,
        DENSE_RANK() OVER (ORDER BY move_qty NULLS LAST) AS move_qty_dense_rank_2
    FROM with_children
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(move_qty_seq - 4) AS move_qty_seq_distance,
        DATEDIFF(DATE '2026-01-01', move_moved_at) AS days_since_move_moved_at,
        DATEDIFF(DATE '2026-01-01', move_moved_at) AS days_since_move_moved_at_2
    FROM with_windows
)
-- Subtotals with GROUPING SETS
SELECT
    warehouse_id,
    move_qty_seq,
    move_type,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
    COUNT(*) AS cnt
FROM annotated
GROUP BY GROUPING SETS ((warehouse_id, move_qty_seq, move_type), (warehouse_id), (move_qty_seq))
