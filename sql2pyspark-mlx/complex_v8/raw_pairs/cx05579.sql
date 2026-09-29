WITH base AS (
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
        ROUND(unit_price * 0.25, 2) AS unit_price_adj,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        qty,
        moved_at,
        operator,
        unit_price_adj,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY operator, move_type ORDER BY moved_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        MAX(qty) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_max_qty,
        DENSE_RANK() OVER (PARTITION BY operator, move_type ORDER BY moved_at ASC NULLS LAST) AS moved_at_dense_rank,
        COUNT(*) OVER (PARTITION BY warehouse_id, operator) AS rows_in_warehouse_id_operator,
        COUNT(*) OVER (PARTITION BY move_type, operator) AS rows_in_move_type_operator,
        MIN(unit_price) OVER (PARTITION BY move_type, operator) AS move_type_operator_min_unit_price,
        MIN(unit_price) OVER (PARTITION BY warehouse_id ORDER BY unit_price_adj ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far,
        ROUND(unit_price_adj / NULLIF(SUM(unit_price_adj) OVER (PARTITION BY move_type, operator), 0), 4) AS unit_price_adj_share_of_move_type_operator,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id, operator)) AS distinct_move_type_in_warehouse_id_operator,
        ROUND(unit_price_adj / NULLIF(SUM(unit_price_adj) OVER (PARTITION BY warehouse_id, operator), 0), 4) AS unit_price_adj_share_of_warehouse_id_operator,
        ROUND(unit_price_adj - AVG(unit_price_adj) OVER w_move_type_operator, 2) AS unit_price_adj_vs_move_type_operator_avg
    FROM base
    WINDOW w_move_type_operator AS (PARTITION BY move_type, operator)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        moved_at,
        operator,
        last_known_moved_at,
        rows_in_warehouse_id_operator,
        rows_in_move_type_operator,
        move_type_operator_min_unit_price,
        min_unit_price_so_far,
        distinct_move_type_in_warehouse_id_operator,
        ROUND((UNIX_TIMESTAMP(last_known_moved_at) - UNIX_TIMESTAMP(moved_at)) / 60, 1) AS minutes_moved_at_to_last_known_moved_at,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        ROUND(unit_price_adj / NULLIF(moved_at_dense_rank, 0), 3) AS unit_price_adj_per_moved_at_dense_rank
    FROM ranked
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        moved_at,
        operator,
        last_known_moved_at,
        rows_in_warehouse_id_operator,
        rows_in_move_type_operator,
        move_type_operator_min_unit_price,
        min_unit_price_so_far,
        distinct_move_type_in_warehouse_id_operator,
        qty_bucket,
        unit_price_adj_per_moved_at_dense_rank,
        NTH_VALUE(last_known_moved_at, 2) OVER (PARTITION BY distinct_move_type_in_warehouse_id_operator, operator ORDER BY rows_in_move_type_operator DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_moved_at,
        RANK() OVER w_distinct_move_type_in_wareho_2 AS rows_in_move_type_operator_rank,
        rows_in_move_type_operator - LAG(rows_in_move_type_operator) OVER (PARTITION BY distinct_move_type_in_warehouse_id_operator, operator ORDER BY rows_in_move_type_operator DESC, id) AS rows_in_move_type_operator_change
    FROM enriched
    WINDOW w_distinct_move_type_in_wareho_2 AS (PARTITION BY distinct_move_type_in_warehouse_id_operator, operator ORDER BY rows_in_move_type_operator DESC, id)
)
SELECT
    id,
    operator,
    last_known_moved_at,
    rows_in_warehouse_id_operator,
    rows_in_move_type_operator,
    distinct_move_type_in_warehouse_id_operator,
    qty_bucket,
    unit_price_adj_per_moved_at_dense_rank,
    second_last_known_moved_at,
    rows_in_move_type_operator_rank
FROM ranked_2
WHERE move_type_operator_min_unit_price BETWEEN 1250.8 AND 4000.2
ORDER BY qty_bucket DESC NULLS FIRST
