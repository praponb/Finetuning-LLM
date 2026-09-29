WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 6 AND 8
      AND qty < 355
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER w_operator_warehouse_id) AS distinct_sku_id_in_operator_warehouse_id,
        DENSE_RANK() OVER (PARTITION BY move_type ORDER BY moved_at, id) AS moved_at_dense_rank,
        unit_price - LAG(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price DESC, id) AS unit_price_change,
        LAG(qty, 3) OVER w_warehouse_id_move_type_by_un AS prev3_qty,
        COUNT(*) OVER (PARTITION BY sku_id, operator) AS rows_in_sku_id_operator,
        SUM(unit_price) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_sum_6_prior,
        LEAD(qty) OVER w_sku_id_by_moved_at AS next_qty,
        LEAD(qty) OVER w_sku_id_by_moved_at_2 AS next_qty_2,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id
    FROM recent_moves
    WINDOW w_operator_warehouse_id AS (PARTITION BY operator, warehouse_id), w_warehouse_id_move_type_by_un AS (PARTITION BY warehouse_id, move_type ORDER BY unit_price, id), w_sku_id_by_moved_at AS (PARTITION BY sku_id ORDER BY moved_at, id), w_sku_id_by_moved_at_2 AS (PARTITION BY sku_id ORDER BY moved_at DESC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        distinct_sku_id_in_operator_warehouse_id,
        unit_price_change,
        prev3_qty,
        rows_in_sku_id_operator,
        unit_price_sum_6_prior,
        next_qty,
        next_qty_2,
        rows_in_sku_id
    FROM windowed
    WHERE moved_at_dense_rank <= 5
),
totals AS (
    -- Summarise per group
    SELECT
        sku_id,
        COUNT(*) FILTER (WHERE move_type IS NOT NULL) AS n_move_type_is_not_null,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        ROUND(AVG(next_qty_2), 2) AS avg_next_qty_2
    FROM top_per_group
    GROUP BY sku_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(n_move_type_is_not_null, 3) OVER (PARTITION BY n_move_type_is_not_null, sku_id ORDER BY n_move_type_is_not_null, sku_id) AS prev3_n_move_type_is_not_null,
        LEAD(n_move_type_is_not_null) OVER (PARTITION BY n_move_type_is_not_null, sku_id ORDER BY n_move_type_is_not_null, sku_id) AS next_n_move_type_is_not_null,
        COUNT(*) OVER (PARTITION BY n_move_type_is_not_null, sku_id ORDER BY n_move_type_is_not_null, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER (PARTITION BY n_move_type_is_not_null, sku_id ORDER BY n_move_type_is_not_null, sku_id) AS n_move_type_is_not_null_rank
    FROM totals
)
-- Final projection
SELECT
    sku_id,
    operator_list,
    avg_next_qty_2,
    prev3_n_move_type_is_not_null,
    next_n_move_type_is_not_null,
    running_count,
    n_move_type_is_not_null_rank,
    GREATEST(avg_next_qty_2, n_move_type_is_not_null) AS greatest_avg_next_qty_2_n_move_type_is_not_null
FROM with_trends
WHERE next_n_move_type_is_not_null BETWEEN 2 AND 6
ORDER BY greatest_avg_next_qty_2_n_move_type_is_not_null, sku_id
LIMIT 10
