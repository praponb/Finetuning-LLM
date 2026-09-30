WITH scoped_moves AS (
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
        CONCAT_WS(': ', operator, move_type) AS operator_move_type_label,
        YEAR(moved_at) AS moved_at_year
    FROM stock_moves
    WHERE sku_id >= 12
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, operator ORDER BY unit_price DESC), 3) AS unit_price_cume_dist,
        MAX(unit_price) OVER (ORDER BY moved_at_year DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_max_3_prior,
        RANK() OVER (PARTITION BY moved_at_year ORDER BY moved_at) AS moved_at_rank,
        SIZE(COLLECT_SET(sku_id) OVER w_move_type_operator) AS distinct_sku_id_in_move_type_operator,
        LEAD(moved_at) OVER (PARTITION BY moved_at_year ORDER BY moved_at, id) AS next_moved_at,
        NTILE(5) OVER (PARTITION BY sku_id ORDER BY qty NULLS LAST, id) AS qty_quintile,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY move_type, operator)) AS distinct_warehouse_id_in_move_type_operator,
        ROW_NUMBER() OVER (ORDER BY moved_at_year DESC, id) AS rn,
        DENSE_RANK() OVER (PARTITION BY operator, move_type ORDER BY qty ASC NULLS LAST, id) AS qty_dense_rank
    FROM scoped_moves
    WINDOW w_move_type_operator AS (PARTITION BY move_type, operator)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY qty_quintile, rn)) AS distinct_move_type_in_qty_quintile_rn,
        LEAD(unit_price_max_3_prior, 3) OVER (ORDER BY distinct_warehouse_id_in_move_type_operator DESC, id) AS next3_unit_price_max_3_prior,
        ROUND(AVG(moved_at_year) OVER (ORDER BY distinct_warehouse_id_in_move_type_operator DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS moved_at_year_avg_1_prior,
        ROW_NUMBER() OVER (PARTITION BY YEAR(next_moved_at), distinct_warehouse_id_in_move_type_operator ORDER BY moved_at_rank DESC, id) AS rn_2,
        FIRST_VALUE(qty) OVER (PARTITION BY YEAR(next_moved_at), distinct_warehouse_id_in_move_type_operator ORDER BY moved_at_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROUND(AVG(qty_dense_rank) OVER (PARTITION BY YEAR(next_moved_at), distinct_warehouse_id_in_move_type_operator ORDER BY moved_at_rank DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_dense_rank_avg_4
    FROM sequenced
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price,
    operator_move_type_label,
    distinct_sku_id_in_move_type_operator,
    next_moved_at,
    distinct_warehouse_id_in_move_type_operator,
    qty_dense_rank,
    distinct_move_type_in_qty_quintile_rn,
    moved_at_year_avg_1_prior,
    rn_2,
    first_qty,
    qty_dense_rank_avg_4,
    FLOOR(qty_dense_rank / 2) * 2 AS qty_dense_rank_bucket,
    moved_at + INTERVAL 2 HOURS AS moved_at_plus_2h
FROM with_trends
WHERE distinct_move_type_in_qty_quintile_rn > 2
ORDER BY qty_dense_rank_bucket, rn_2, id
LIMIT 20
