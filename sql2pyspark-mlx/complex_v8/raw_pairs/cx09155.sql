WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj
    FROM stock_moves
    WHERE qty <= 271
      AND operator NOT IN ('op_eve', 'op_chan')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        unit_price_adj,
        RANK() OVER w_warehouse_id_by_qty_ties AS qty_rank,
        MAX(unit_price_adj) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_max_unit_price_adj,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type, warehouse_id), 0), 4) AS unit_price_share_of_move_type_warehouse_id,
        ROUND(qty - AVG(qty) OVER (PARTITION BY move_type, warehouse_id), 2) AS qty_vs_move_type_warehouse_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id ORDER BY qty, id), 3) AS qty_cume_dist,
        LAG(unit_price) OVER w_all_by_moved_at AS prev_unit_price,
        ROW_NUMBER() OVER w_all_by_unit_price AS unit_price_seq,
        MIN(qty) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_min_qty,
        DENSE_RANK() OVER (ORDER BY moved_at DESC) AS moved_at_dense_rank,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type, warehouse_id)) AS distinct_sku_id_in_move_type_warehouse_id,
        ROW_NUMBER() OVER (ORDER BY moved_at DESC, id) AS rn,
        COUNT(*) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(unit_price_adj, 2) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_adj
    FROM scoped_moves
    WINDOW w_warehouse_id_by_qty_ties AS (PARTITION BY warehouse_id ORDER BY qty), w_all_by_moved_at AS (ORDER BY moved_at DESC, id), w_all_by_unit_price AS (ORDER BY unit_price ASC, id), w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(qty) OVER (PARTITION BY moved_at_dense_rank) AS moved_at_dense_rank_max_qty,
        NTH_VALUE(moved_at_dense_rank, 2) OVER (PARTITION BY move_type ORDER BY move_type_warehouse_id_min_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_dense_rank,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY moved_at_dense_rank)) AS distinct_sku_id_in_moved_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY move_type_warehouse_id_min_qty, id) AS rn_2,
        LAG(unit_price, 3) OVER (PARTITION BY sku_id ORDER BY qty_vs_move_type_warehouse_id_avg DESC, id) AS prev3_unit_price,
        SUM(qty_rank) OVER (PARTITION BY move_type ORDER BY move_type_warehouse_id_min_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS qty_rank_sum_3
    FROM with_windows
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    moved_at,
    unit_price,
    unit_price_adj,
    qty_rank,
    move_type_warehouse_id_max_unit_price_adj,
    unit_price_share_of_move_type_warehouse_id,
    qty_vs_move_type_warehouse_id_avg,
    qty_cume_dist,
    move_type_warehouse_id_min_qty,
    moved_at_dense_rank,
    distinct_sku_id_in_move_type_warehouse_id,
    rn,
    second_unit_price_adj,
    moved_at_dense_rank_max_qty,
    second_moved_at_dense_rank,
    distinct_sku_id_in_moved_at_dense_rank,
    rn_2,
    prev3_unit_price,
    qty_rank_sum_3,
    LEAST(rn, prev3_unit_price) AS least_rn_prev3_unit_price
FROM metrics
ORDER BY moved_at NULLS LAST, id
LIMIT 10
