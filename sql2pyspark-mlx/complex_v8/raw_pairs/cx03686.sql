WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator = 'op_eve'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator,
        MAX(qty) OVER (PARTITION BY operator, sku_id ORDER BY qty, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_max_2_prior,
        DENSE_RANK() OVER (PARTITION BY move_type ORDER BY qty) AS qty_dense_rank,
        SIZE(COLLECT_SET(sku_id) OVER w_move_type_operator) AS distinct_sku_id_in_move_type_operator,
        qty - LAG(qty) OVER (PARTITION BY move_type ORDER BY qty, id) AS qty_change,
        LAST_VALUE(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        RANK() OVER w_warehouse_id_by_unit_price AS unit_price_rank,
        unit_price - LAG(unit_price) OVER (PARTITION BY operator, sku_id ORDER BY qty, id) AS unit_price_change,
        SUM(unit_price) OVER (PARTITION BY move_type ORDER BY qty, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS unit_price_sum_1_prior,
        NTILE(3) OVER w_operator_sku_id_by_qty AS qty_tercile,
        DENSE_RANK() OVER (ORDER BY qty DESC, id) AS qty_dense_rank_2,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY qty, id) AS qty_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type ORDER BY qty), 3) AS qty_cume_dist
    FROM filtered_moves
    WINDOW w_move_type_operator AS (PARTITION BY move_type, operator), w_warehouse_id_by_unit_price AS (PARTITION BY warehouse_id ORDER BY unit_price ASC, id), w_operator_sku_id_by_qty AS (PARTITION BY operator, sku_id ORDER BY qty, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        rows_in_operator,
        qty_max_2_prior,
        qty_dense_rank,
        distinct_sku_id_in_move_type_operator,
        qty_change,
        last_unit_price,
        unit_price_change,
        unit_price_sum_1_prior,
        qty_tercile,
        qty_dense_rank_2,
        first_known_qty,
        qty_seq,
        qty_cume_dist
    FROM sequenced
    WHERE unit_price_rank <= 2
      AND sku_id IS NOT NULL
),
grouped AS (
    -- Group-level totals
    SELECT
        qty_dense_rank,
        warehouse_id,
        qty_dense_rank_2,
        COUNT(DISTINCT operator) AS distinct_operators,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        COUNT(*) FILTER (WHERE move_type = 'OUTBOUND') AS n_move_type_outbound,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM first_rows
    GROUP BY qty_dense_rank, warehouse_id, qty_dense_rank_2
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        qty_dense_rank,
        warehouse_id,
        qty_dense_rank_2,
        n_move_type_outbound,
        operator_list,
        SUM(distinct_operators) OVER (ORDER BY n_move_type_outbound DESC, qty_dense_rank, warehouse_id, qty_dense_rank_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_distinct_operators,
        RANK() OVER (ORDER BY n_move_type_outbound DESC) AS n_move_type_outbound_rank,
        ROUND(CUME_DIST() OVER w_all_by_n_move_type_outbound_2, 3) AS n_move_type_outbound_cume_dist
    FROM grouped
    WINDOW w_all_by_n_move_type_outbound_2 AS (ORDER BY n_move_type_outbound DESC)
)
SELECT
    qty_dense_rank,
    warehouse_id,
    qty_dense_rank_2,
    n_move_type_outbound,
    operator_list,
    running_distinct_operators,
    n_move_type_outbound_cume_dist,
    COALESCE(qty_dense_rank_2, 0) AS qty_dense_rank_2_filled,
    ROUND(qty_dense_rank / NULLIF(n_move_type_outbound, 0), 3) AS qty_dense_rank_per_n_move_type_outbound
FROM windowed
ORDER BY qty_dense_rank_2_filled
