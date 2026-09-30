WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.qty_plus_unit_price,
        sk.is_active AS sku_is_active
    FROM prepared_moves s
    JOIN skus sk ON sk.id = s.sku_id AND sk.unit_cost >= 60.9
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(qty_plus_unit_price / NULLIF(SUM(qty_plus_unit_price) OVER (PARTITION BY operator, sku_id), 0), 4) AS qty_plus_unit_price_share_of_operator_sku_id,
        qty_plus_unit_price - LAG(qty_plus_unit_price) OVER (PARTITION BY warehouse_id ORDER BY qty_plus_unit_price DESC NULLS FIRST, id) AS qty_plus_unit_price_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY qty_plus_unit_price ASC, id), 3) AS qty_plus_unit_price_percent_rank,
        LEAD(qty_plus_unit_price, 2) OVER (ORDER BY qty_plus_unit_price ASC, id) AS next2_qty_plus_unit_price,
        ROUND(AVG(qty_plus_unit_price) OVER (PARTITION BY operator, sku_id), 2) AS operator_sku_id_avg_qty_plus_unit_price,
        MIN(qty_plus_unit_price) OVER (PARTITION BY warehouse_id ORDER BY qty_plus_unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_plus_unit_price_so_far,
        ROUND(AVG(qty_plus_unit_price) OVER (PARTITION BY move_type ORDER BY qty_plus_unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_plus_unit_price_avg_2_prior,
        COUNT(*) OVER w_operator_sku_id AS rows_in_operator_sku_id,
        DENSE_RANK() OVER w_warehouse_id_by_qty_plus_uni AS qty_plus_unit_price_dense_rank,
        SUM(qty_plus_unit_price) OVER (PARTITION BY move_type ORDER BY qty_plus_unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty_plus_unit_price
    FROM with_parent_sku
    WINDOW w_operator_sku_id AS (PARTITION BY operator, sku_id), w_warehouse_id_by_qty_plus_uni AS (PARTITION BY warehouse_id ORDER BY qty_plus_unit_price DESC NULLS FIRST, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        move_type,
        qty_plus_unit_price_dense_rank,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        SUM(CASE WHEN operator NOT IN ('op_bee', 'op_dew') THEN 1 ELSE 0 END) AS n_operator_not_in_op_bee_op_de,
        SUM(rows_in_operator_sku_id) AS total_rows_in_operator_sku_id,
        COUNT(*) AS n_rows
    FROM windowed
    GROUP BY move_type, qty_plus_unit_price_dense_rank
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(distinct_sku_ids) IGNORE NULLS OVER (PARTITION BY n_operator_not_in_op_bee_op_de, distinct_sku_ids ORDER BY total_rows_in_operator_sku_id, move_type, qty_plus_unit_price_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_sku_ids,
        ROUND(AVG(distinct_sku_ids) OVER (PARTITION BY qty_plus_unit_price_dense_rank, n_rows ORDER BY n_operator_not_in_op_bee_op_de DESC, move_type, qty_plus_unit_price_dense_rank ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS distinct_sku_ids_avg_4,
        ROUND(CUME_DIST() OVER (PARTITION BY qty_plus_unit_price_dense_rank, n_rows ORDER BY n_operator_not_in_op_bee_op_de DESC), 3) AS n_operator_not_in_op_bee_op_de_cume_dist
    FROM rollup_base
)
SELECT
    move_type,
    qty_plus_unit_price_dense_rank,
    distinct_sku_ids,
    n_operator_not_in_op_bee_op_de,
    total_rows_in_operator_sku_id,
    distinct_sku_ids_avg_4,
    n_operator_not_in_op_bee_op_de_cume_dist,
    CONCAT(move_type, '#', CAST(n_rows AS STRING)) AS move_type_ref,
    INITCAP(TRIM(move_type)) AS move_type_clean
FROM with_windows
ORDER BY move_type NULLS LAST
