WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-08 12:00:00'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type, warehouse_id)) AS distinct_sku_id_in_move_type_warehouse_id,
        ROW_NUMBER() OVER w_sku_id_by_unit_price AS seq_no,
        MAX(qty) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_max_qty,
        MIN(qty) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_min_qty,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY move_type, warehouse_id), 0), 4) AS qty_share_of_move_type_warehouse_id,
        ROUND(CUME_DIST() OVER (ORDER BY qty NULLS LAST, id), 3) AS qty_cume_dist,
        MAX(unit_price) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_max_unit_price,
        LAG(qty, 1, 0) OVER (ORDER BY unit_price, id) AS prev_qty,
        COUNT(*) OVER (PARTITION BY move_type, warehouse_id) AS rows_in_move_type_warehouse_id,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_2
    FROM filtered
    WINDOW w_sku_id_by_unit_price AS (PARTITION BY sku_id ORDER BY unit_price, id), w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(second_unit_price_2) OVER (ORDER BY move_type_warehouse_id_min_qty DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS second_unit_price_2_sum_2,
        SUM(seq_no) OVER (PARTITION BY warehouse_id ORDER BY rows_in_move_type_warehouse_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        MAX(second_unit_price) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_max_second_unit_price,
        DENSE_RANK() OVER w_all_by_move_type_warehouse_i_2 AS move_type_warehouse_id_min_qty_dense_rank,
        MIN(qty) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_min_qty,
        MIN(qty_cume_dist) OVER w_sku_id_move_type AS sku_id_move_type_min_qty_cume_dist,
        MAX(qty_cume_dist) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_max_qty_cume_dist
    FROM with_trends
    WINDOW w_all_by_move_type_warehouse_i_2 AS (ORDER BY move_type_warehouse_id_min_qty DESC, id), w_sku_id_move_type AS (PARTITION BY sku_id, move_type)
),
totals AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        seq_no,
        move_type_warehouse_id_min_qty_dense_rank,
        SUM(rows_in_move_type_warehouse_id) FILTER (WHERE operator IS NOT NULL) AS rows_in_move_type_warehouse_id_operator_is_not_n,
        ROUND(SUM(second_unit_price), 2) AS total_second_unit_price,
        COUNT(*) AS cnt
    FROM sequenced
    WHERE sku_id_move_type_max_second_unit_price <= 4250.1
    GROUP BY warehouse_id, seq_no, move_type_warehouse_id_min_qty_dense_rank
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        warehouse_id,
        seq_no,
        move_type_warehouse_id_min_qty_dense_rank,
        rows_in_move_type_warehouse_id_operator_is_not_n,
        total_second_unit_price,
        cnt,
        ROUND(seq_no / NULLIF(SUM(seq_no) OVER w_warehouse_id, 0), 4) AS seq_no_share_of_warehouse_id,
        LAST_VALUE(total_second_unit_price) IGNORE NULLS OVER (ORDER BY rows_in_move_type_warehouse_id_operator_is_not_n ASC, warehouse_id, seq_no, move_type_warehouse_id_min_qty_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_total_second_unit_price,
        ROUND(total_second_unit_price / NULLIF(SUM(total_second_unit_price) OVER w_warehouse_id, 0), 4) AS total_second_unit_price_share_of_warehouse_id,
        ROUND(AVG(cnt) OVER w_warehouse_id, 2) AS warehouse_id_avg_cnt
    FROM totals
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
)
-- Final projection
SELECT
    warehouse_id,
    seq_no,
    move_type_warehouse_id_min_qty_dense_rank,
    rows_in_move_type_warehouse_id_operator_is_not_n,
    total_second_unit_price,
    cnt,
    seq_no_share_of_warehouse_id,
    last_known_total_second_unit_price,
    total_second_unit_price_share_of_warehouse_id,
    warehouse_id_avg_cnt,
    CASE WHEN total_second_unit_price >= 2550.2 THEN 'HIGH' WHEN total_second_unit_price >= 750.8 THEN 'MEDIUM' ELSE 'LOW' END AS total_second_unit_price_tier,
    total_second_unit_price + seq_no AS total_second_unit_price_plus_seq_no
FROM windowed
ORDER BY seq_no, move_type_warehouse_id_min_qty_dense_rank
