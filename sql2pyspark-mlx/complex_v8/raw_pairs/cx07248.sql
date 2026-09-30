WITH filtered AS (
    -- Filter stock_moves
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
    WHERE move_type IS NOT NULL
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
),
filtered_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_activity s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.qty > 191
        )
),
summary AS (
    -- Summarise per group
    SELECT
        move_type,
        warehouse_id,
        SUM(qty) AS total_qty,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list
    FROM filtered_2
    WHERE move_type = 'ADJUST'
    GROUP BY move_type, warehouse_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(total_qty, 3) OVER w_all_by_total_qty AS prev3_total_qty,
        NTILE(5) OVER (ORDER BY total_qty ASC, move_type, warehouse_id) AS total_qty_quintile,
        MIN(total_qty) OVER (ORDER BY total_qty DESC, move_type, warehouse_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_total_qty_so_far,
        LAST_VALUE(total_qty) OVER (ORDER BY total_qty ASC, move_type, warehouse_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_qty
    FROM summary
    WINDOW w_all_by_total_qty AS (ORDER BY total_qty ASC, move_type, warehouse_id)
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.operator <> 'op_eve'
              AND TO_DATE(st.moved_at) < DATE '2025-03-08'
        )
)
SELECT
    move_type,
    total_qty,
    prev3_total_qty,
    total_qty_quintile,
    min_total_qty_so_far,
    last_total_qty
FROM without_matches
WHERE SIZE(move_type_list) >= 1
ORDER BY move_type
