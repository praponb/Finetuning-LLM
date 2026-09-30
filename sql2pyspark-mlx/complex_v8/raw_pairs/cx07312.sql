WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        CASE WHEN move_type IN ('TRANSFER', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE qty BETWEEN 111 AND 469
      AND unit_price > 61.2
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) AS move_count,
        COUNT(*) AS move_count_2,
        COUNT(*) FILTER (WHERE operator NOT IN ('op_eve', 'op_chan')) AS move_operator_count
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM valid_moves s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.move_type_group,
        s.move_count,
        s.move_count_2,
        st.id AS move_id,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id,
        st.unit_price AS move_unit_price,
        st.moved_at AS move_moved_at
    FROM with_counts s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        move_count_2,
        move_id,
        move_operator,
        move_sku_id,
        move_unit_price,
        move_moved_at,
        COUNT(*) OVER w_move_moved_at_month_move_typ AS rows_in_move_moved_at_month_move_type_group,
        MIN(move_count_2) OVER (PARTITION BY warehouse_id ORDER BY qty, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_move_count_2_so_far,
        RANK() OVER (PARTITION BY warehouse_id ORDER BY qty) AS qty_rank,
        RANK() OVER (ORDER BY qty DESC) AS qty_rank_2,
        MAX(move_count_2) OVER w_move_moved_at_month_move_typ AS move_moved_at_month_move_type_group_max_move_cou,
        ROUND(move_unit_price - AVG(move_unit_price) OVER (PARTITION BY MONTH(move_moved_at), move_type_group), 2) AS move_unit_price_vs_move_moved_at_month_move_type
    FROM detail_rows_moves
    WINDOW w_move_moved_at_month_move_typ AS (PARTITION BY MONTH(move_moved_at), move_type_group)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        move_id,
        move_operator,
        move_sku_id,
        move_moved_at,
        min_move_count_2_so_far,
        qty_rank_2,
        move_moved_at_month_move_type_group_max_move_cou,
        move_unit_price_vs_move_moved_at_month_move_type,
        qty - LAG(qty) OVER (PARTITION BY move_operator, move_sku_id ORDER BY move_unit_price_vs_move_moved_at_month_move_type DESC, id, move_id) AS qty_change,
        ROUND(min_move_count_2_so_far / NULLIF(SUM(min_move_count_2_so_far) OVER (PARTITION BY qty_rank, YEAR(move_moved_at)), 0), 4) AS min_move_count_2_so_far_share_of_qty_rank_move_m,
        MAX(min_move_count_2_so_far) OVER (PARTITION BY move_operator, move_sku_id ORDER BY move_unit_price_vs_move_moved_at_month_move_type DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_min_move_count_2_so_far_so_far,
        COUNT(*) OVER w_qty_rank_move_moved_at_year AS rows_in_qty_rank_move_moved_at_year,
        DENSE_RANK() OVER (PARTITION BY move_operator, move_sku_id ORDER BY move_unit_price_vs_move_moved_at_month_move_type DESC, id, move_id) AS move_unit_price_vs_move_moved_at_month_move_type_2
    FROM ranked
    WINDOW w_qty_rank_move_moved_at_year AS (PARTITION BY qty_rank, YEAR(move_moved_at))
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(qty_rank_2) OVER w_sku_id_warehouse_id, 2) AS sku_id_warehouse_id_avg_qty_rank_2,
        COUNT(*) OVER w_sku_id_warehouse_id AS rows_in_sku_id_warehouse_id,
        ROUND(CUME_DIST() OVER (PARTITION BY move_unit_price_vs_move_moved_at_month_move_type_2, move_sku_id ORDER BY max_min_move_count_2_so_far_so_far), 3) AS max_min_move_count_2_so_far_so_far_cume_dist,
        MIN(min_move_count_2_so_far) OVER (PARTITION BY sku_id, warehouse_id) AS sku_id_warehouse_id_min_min_move_count_2_so_far,
        ROUND(AVG(qty_rank_2) OVER (PARTITION BY move_unit_price_vs_move_moved_at_month_move_type_2, move_sku_id ORDER BY max_min_move_count_2_so_far_so_far, id, move_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_rank_2_avg_2_prior
    FROM windowed
    WINDOW w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        move_id,
        move_operator,
        move_sku_id,
        move_moved_at,
        min_move_count_2_so_far,
        min_move_count_2_so_far_share_of_qty_rank_move_m,
        rows_in_qty_rank_move_moved_at_year,
        sku_id_warehouse_id_avg_qty_rank_2,
        rows_in_sku_id_warehouse_id,
        max_min_move_count_2_so_far_so_far_cume_dist,
        sku_id_warehouse_id_min_min_move_count_2_so_far,
        qty_rank_2_avg_2_prior,
        LENGTH(TRIM(move_type)) AS move_type_len,
        LENGTH(TRIM(operator)) AS operator_len,
        min_move_count_2_so_far - qty_rank_2_avg_2_prior AS min_move_count_2_so_far_minus_qty_rank_2_avg_2_p
    FROM sequenced
    WHERE max_min_move_count_2_so_far_so_far_cume_dist >= 0.5
)
-- Final projection
SELECT
    id,
    move_type,
    operator,
    move_id,
    move_sku_id,
    move_moved_at,
    min_move_count_2_so_far,
    rows_in_qty_rank_move_moved_at_year,
    rows_in_sku_id_warehouse_id,
    max_min_move_count_2_so_far_so_far_cume_dist,
    sku_id_warehouse_id_min_min_move_count_2_so_far,
    qty_rank_2_avg_2_prior,
    move_type_len,
    min_move_count_2_so_far_minus_qty_rank_2_avg_2_p
FROM annotated
WHERE operator NOT IN ('op_bee', 'op_ann')
ORDER BY rows_in_sku_id_warehouse_id DESC, max_min_move_count_2_so_far_so_far_cume_dist DESC
