WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        HOUR(moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE warehouse_id >= 2
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM valid_moves s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
        )
),
with_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.operator_or_unknown,
        s.moved_at_hour,
        w.opened_on AS warehouse_opened_on
    FROM qualified s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_sku_id_moved_at_hour_by_ware, 3) AS warehouse_opened_on_cume_dist,
        NTH_VALUE(warehouse_opened_on, 2) OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        ROUND(PERCENT_RANK() OVER w_all_by_qty, 3) AS qty_percent_rank
    FROM with_details
    WINDOW w_sku_id_moved_at_hour_by_ware AS (PARTITION BY sku_id, moved_at_hour ORDER BY warehouse_opened_on DESC), w_all_by_qty AS (ORDER BY qty DESC, id)
),
without_matches AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND SIZE(sk.labels) >= 2
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, qty_percent_rank, warehouse_opened_on_cume_dist, unit_price
    FROM without_matches
) src
UNPIVOT (
    amount FOR measure IN (qty_percent_rank, warehouse_opened_on_cume_dist, unit_price)
) unp
ORDER BY id, measure
