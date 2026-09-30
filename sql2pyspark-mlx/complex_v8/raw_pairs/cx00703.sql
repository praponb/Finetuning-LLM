WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN move_type IN ('ADJUST', 'INBOUND') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR move_type IS NOT NULL)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        move_type_group,
        unit_price_adj,
        ROUND(unit_price_adj / NULLIF(SUM(unit_price_adj) OVER w_warehouse_id, 0), 4) AS unit_price_adj_share_of_warehouse_id,
        unit_price_adj - LAG(unit_price_adj) OVER (PARTITION BY move_type ORDER BY qty, id) AS unit_price_adj_change,
        RANK() OVER (PARTITION BY sku_id, move_type ORDER BY unit_price_adj, id) AS unit_price_adj_rank,
        RANK() OVER (PARTITION BY move_type_group, sku_id ORDER BY unit_price_adj, id) AS unit_price_adj_rank_2,
        ROUND(qty / NULLIF(SUM(qty) OVER w_warehouse_id, 0), 4) AS qty_share_of_warehouse_id,
        LAG(qty, 2) OVER (PARTITION BY move_type ORDER BY qty, id) AS prev2_qty,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY move_type_group, sku_id ORDER BY unit_price_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        DENSE_RANK() OVER (PARTITION BY move_type ORDER BY qty) AS qty_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY qty, id) AS seq_no,
        ROUND(unit_price_adj - AVG(unit_price_adj) OVER w_warehouse_id, 2) AS unit_price_adj_vs_warehouse_id_avg,
        COUNT(*) OVER w_warehouse_id AS rows_in_warehouse_id,
        NTILE(10) OVER (PARTITION BY move_type ORDER BY qty, id) AS qty_decile,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY move_type ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price
    FROM prepared_moves
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_trends
    WHERE seq_no <= 2
),
totals AS (
    -- Summarise per group
    SELECT
        move_type_group,
        move_type,
        COUNT(*) FILTER (WHERE move_type = 'ADJUST') AS n_move_type_adjust,
        MAX(unit_price) AS max_unit_price,
        PERCENTILE_APPROX(qty_dense_rank, 0.5) AS median_qty_dense_rank
    FROM top_rows
    GROUP BY move_type_group, move_type
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(max_unit_price) OVER (PARTITION BY median_qty_dense_rank, move_type ORDER BY max_unit_price DESC, move_type_group, move_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_max_unit_price,
        ROUND(AVG(n_move_type_adjust) OVER (PARTITION BY move_type_group, n_move_type_adjust), 2) AS move_type_group_n_move_type_adjust_avg_n_move_ty,
        FIRST_VALUE(n_move_type_adjust) OVER (PARTITION BY median_qty_dense_rank, move_type ORDER BY max_unit_price DESC, move_type_group, move_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_move_type_adjust,
        COUNT(*) OVER (PARTITION BY move_type_group, n_move_type_adjust) AS rows_in_move_type_group_n_move_type_adjust
    FROM totals
)
SELECT
    move_type_group,
    move_type,
    median_qty_dense_rank,
    move_type_group_n_move_type_adjust_avg_n_move_ty,
    first_n_move_type_adjust,
    CASE WHEN move_type_group IN ('group_b', 'other') THEN 'group_a' WHEN move_type_group = 'group_a' THEN 'group_b' ELSE 'other' END AS move_type_group_group
FROM with_trends_2
WHERE move_type IN ('TRANSFER', 'OUTBOUND')
ORDER BY move_type_group
