WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj
    FROM stock_moves
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        unit_price_adj,
        ROUND(AVG(unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), move_type ORDER BY unit_price_adj, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7,
        SUM(unit_price_adj) OVER (PARTITION BY warehouse_id) AS warehouse_id_sum_unit_price_adj,
        LEAD(unit_price_adj) OVER (ORDER BY unit_price, id) AS next_unit_price_adj,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS unit_price_vs_warehouse_id_avg,
        ROUND(AVG(unit_price_adj) OVER (ORDER BY unit_price, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_adj_avg_1_prior,
        LAG(unit_price, 1, 0) OVER (ORDER BY unit_price, id) AS prev_unit_price,
        LAG(unit_price, 1, 0) OVER (ORDER BY moved_at DESC, id) AS prev_unit_price_2,
        FIRST_VALUE(unit_price_adj) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price_adj,
        ROUND(AVG(unit_price_adj) OVER (PARTITION BY MONTH(moved_at)), 2) AS moved_at_month_avg_unit_price_adj,
        ROUND(qty - AVG(qty) OVER (PARTITION BY MONTH(moved_at)), 2) AS qty_vs_moved_at_month_avg,
        LEAD(unit_price_adj) OVER w_moved_at_month_move_type_by_2 AS next_unit_price_adj_2,
        MAX(qty) OVER (ORDER BY unit_price, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS qty_max_2,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        NTH_VALUE(unit_price_adj, 2) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_adj,
        RANK() OVER w_all_by_moved_at AS moved_at_rank
    FROM prepared_moves
    WINDOW w_moved_at_month_move_type_by_2 AS (PARTITION BY DATE_TRUNC('MONTH', moved_at), move_type ORDER BY unit_price_adj, id), w_all_by_moved_at AS (ORDER BY moved_at DESC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        moved_at,
        unit_price,
        unit_price_adj,
        unit_price_avg_7,
        unit_price_vs_warehouse_id_avg,
        unit_price_adj_avg_1_prior,
        prev_unit_price_2,
        moved_at_month_avg_unit_price_adj,
        next_unit_price_adj_2,
        second_unit_price_adj,
        moved_at_rank,
        LAG(unit_price_avg_7) OVER (PARTITION BY moved_at_rank, warehouse_id ORDER BY moved_at_rank DESC NULLS FIRST, id) AS prev_unit_price_avg_7,
        LEAD(prev_unit_price) OVER (PARTITION BY moved_at_rank, warehouse_id ORDER BY moved_at_rank DESC NULLS FIRST, id) AS next_prev_unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, warehouse_id ORDER BY prev_unit_price_2), 3) AS prev_unit_price_2_cume_dist,
        COUNT(*) OVER (PARTITION BY warehouse_id, sku_id) AS rows_in_warehouse_id_sku_id,
        ROUND(unit_price_adj / NULLIF(SUM(unit_price_adj) OVER w_warehouse_id_sku_id, 0), 4) AS unit_price_adj_share_of_warehouse_id_sku_id
    FROM ranked
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id)
)
SELECT
    id,
    sku_id,
    moved_at,
    unit_price,
    unit_price_adj,
    unit_price_avg_7,
    unit_price_adj_avg_1_prior,
    prev_unit_price_2,
    moved_at_month_avg_unit_price_adj,
    next_unit_price_adj_2,
    moved_at_rank,
    next_prev_unit_price,
    rows_in_warehouse_id_sku_id,
    unit_price_adj_share_of_warehouse_id_sku_id,
    ABS(prev_unit_price_2 - 209.4) AS prev_unit_price_2_distance,
    FLOOR(prev_unit_price_2 / 100) * 100 AS prev_unit_price_2_bucket
FROM windowed
WHERE moved_at < TIMESTAMP '2025-02-12 15:00:00'
