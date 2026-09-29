WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label
    FROM stock_moves
    WHERE move_type <> 'INBOUND'
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        qty,
        unit_price,
        moved_at_label,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id, move_type ORDER BY qty ASC), 3) AS qty_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price DESC, id), 3) AS unit_price_percent_rank,
        ROUND(AVG(qty) OVER w_warehouse_id_move_type, 2) AS warehouse_id_move_type_avg_qty,
        LEAD(unit_price) OVER w_sku_id_move_type_by_qty AS next_unit_price,
        RANK() OVER (PARTITION BY sku_id, move_type ORDER BY qty ASC) AS qty_rank,
        RANK() OVER w_sku_id_move_type_by_qty AS qty_rank_2
    FROM with_activity
    WINDOW w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type), w_sku_id_move_type_by_qty AS (PARTITION BY sku_id, move_type ORDER BY qty ASC, id)
)
-- Subtotals with CUBE
SELECT
    qty_rank,
    qty_rank_2,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    SUM(qty_rank) AS total_qty_rank
FROM metrics
GROUP BY CUBE(qty_rank, qty_rank_2)
ORDER BY qty_rank NULLS LAST, qty_rank_2 NULLS LAST
