WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id >= 4
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(qty - AVG(qty) OVER w_warehouse_id_sku_id, 2) AS qty_vs_warehouse_id_sku_id_avg,
        LEAD(qty) OVER (PARTITION BY move_type ORDER BY qty DESC, id) AS next_qty,
        NTH_VALUE(qty, 2) OVER (PARTITION BY move_type ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM scoped_moves
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id)
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.qty_vs_warehouse_id_sku_id_avg,
        s.next_qty,
        s.second_qty,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.unit_price AS move_unit_price
    FROM with_trends s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
without_matches AS (
    -- Keep rows whose move matches
    SELECT s.*
    FROM with_children_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.id = s.move_id
              AND st.moved_at < TIMESTAMP '2025-03-02 13:00:00'
              AND st.qty BETWEEN 99 AND 470
        )
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM without_matches s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
      AND s.move_id <= 88
),
rollup_base AS (
    -- Group-level totals
    SELECT
        move_id,
        move_move_type,
        COUNT(DISTINCT move_id) AS distinct_move_ids,
        COUNT(DISTINCT move_move_type) AS distinct_move_move_types
    FROM qualified
    GROUP BY move_id, move_move_type
),
with_activity AS (
    -- Keep rows whose move matches
    SELECT s.*
    FROM rollup_base s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.id = s.move_id
              AND st.qty < 300
        )
)
-- Final projection
SELECT
    move_id,
    move_move_type,
    distinct_move_ids,
    LENGTH(TRIM(move_move_type)) AS move_move_type_len
FROM with_activity
