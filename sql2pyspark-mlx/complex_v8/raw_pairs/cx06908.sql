WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE move_type IN ('OUTBOUND', 'TRANSFER', 'ADJUST')
      AND HOUR(moved_at) BETWEEN 7 AND 13
),
joined_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.move_type_len,
        w.opened_on AS warehouse_opened_on,
        sk.unit_cost AS sku_unit_cost
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(move_type_len / NULLIF(SUM(move_type_len) OVER (PARTITION BY warehouse_id), 0), 4) AS move_type_len_share_of_warehouse_id,
        LAG(moved_at) OVER (PARTITION BY move_type_len ORDER BY moved_at, id) AS prev_moved_at,
        DENSE_RANK() OVER w_all_by_moved_at_ties AS moved_at_dense_rank,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        MAX(sku_unit_cost) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_sku_unit_cost,
        LEAD(moved_at, 2) OVER (ORDER BY moved_at, id) AS next2_moved_at,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY moved_at DESC) AS moved_at_dense_rank_2,
        DENSE_RANK() OVER (PARTITION BY move_type_len ORDER BY moved_at) AS moved_at_dense_rank_3
    FROM joined_warehouse
    WINDOW w_all_by_moved_at_ties AS (ORDER BY moved_at)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        move_type_len,
        warehouse_opened_on,
        sku_unit_cost,
        move_type_len_share_of_warehouse_id,
        prev_moved_at,
        moved_at_dense_rank,
        second_warehouse_opened_on,
        warehouse_id_max_sku_unit_cost,
        next2_moved_at,
        moved_at_dense_rank_2
    FROM metrics
    WHERE moved_at_dense_rank_3 = 1
      AND YEAR(warehouse_opened_on) = 2024
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        sku_unit_cost,
        prev_moved_at,
        moved_at_dense_rank,
        second_warehouse_opened_on,
        warehouse_id_max_sku_unit_cost,
        NTILE(4) OVER (PARTITION BY move_type_len ORDER BY warehouse_id_max_sku_unit_cost ASC NULLS LAST, id) AS warehouse_id_max_sku_unit_cost_quartile,
        MIN(move_type_len) OVER (PARTITION BY move_type_len ORDER BY warehouse_id_max_sku_unit_cost ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_move_type_len_so_far,
        ROUND(AVG(moved_at_dense_rank) OVER (PARTITION BY move_type_len, warehouse_id ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS moved_at_dense_rank_avg_7
    FROM leaders
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', second_warehouse_opened_on) AS second_warehouse_opened_on_month,
        sku_id,
        warehouse_id,
        MIN(sku_unit_cost) AS min_sku_unit_cost,
        COUNT(*) AS n_rows,
        SUM(min_move_type_len_so_far) AS total_min_move_type_len_so_far,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(min_move_type_len_so_far) AS max_min_move_type_len_so_far
    FROM with_trends
    WHERE warehouse_id_max_sku_unit_cost_quartile < 7
    GROUP BY DATE_TRUNC('MONTH', second_warehouse_opened_on), sku_id, warehouse_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(distinct_ids / NULLIF(SUM(distinct_ids) OVER (PARTITION BY distinct_ids), 0), 4) AS distinct_ids_share_of_distinct_ids,
        ROUND(CUME_DIST() OVER w_n_rows_by_total_min_move_typ, 3) AS total_min_move_type_len_so_far_cume_dist
    FROM stats
    WINDOW w_n_rows_by_total_min_move_typ AS (PARTITION BY n_rows ORDER BY total_min_move_type_len_so_far)
)
SELECT
    second_warehouse_opened_on_month,
    sku_id,
    warehouse_id,
    min_sku_unit_cost,
    max_min_move_type_len_so_far,
    distinct_ids_share_of_distinct_ids,
    total_min_move_type_len_so_far_cume_dist
FROM ranked
ORDER BY warehouse_id
