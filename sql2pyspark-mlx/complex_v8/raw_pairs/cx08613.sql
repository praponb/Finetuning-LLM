WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE operator IN ('op_chan', 'op_bee', 'op_ann')
      AND qty < 275
),
with_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        w.capacity AS warehouse_capacity
    FROM base s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        warehouse_capacity,
        ROUND(qty - AVG(qty) OVER (PARTITION BY move_type), 2) AS qty_vs_move_type_avg,
        ROUND(PERCENT_RANK() OVER w_sku_id_by_warehouse_capacity, 3) AS warehouse_capacity_percent_rank,
        LEAD(qty) OVER (PARTITION BY sku_id ORDER BY warehouse_capacity DESC, id) AS next_qty,
        MAX(warehouse_capacity) OVER (PARTITION BY move_type) AS move_type_max_warehouse_capacity,
        LEAD(qty) OVER (ORDER BY qty DESC, id) AS next_qty_2,
        ROW_NUMBER() OVER (ORDER BY qty DESC, id) AS seq_no,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id, sku_id), 2) AS warehouse_id_sku_id_avg_qty,
        RANK() OVER w_all_by_warehouse_capacity_ti AS warehouse_capacity_rank
    FROM with_details_warehouse
    WINDOW w_sku_id_by_warehouse_capacity AS (PARTITION BY sku_id ORDER BY warehouse_capacity DESC, id), w_all_by_warehouse_capacity_ti AS (ORDER BY warehouse_capacity DESC)
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    next_qty,
    move_type_max_warehouse_capacity,
    next_qty_2,
    seq_no,
    warehouse_capacity_rank,
    FLOOR(qty_vs_move_type_avg / 250) * 250 AS qty_vs_move_type_avg_bucket
FROM ranked
ORDER BY warehouse_capacity_rank DESC NULLS FIRST, id DESC NULLS FIRST
