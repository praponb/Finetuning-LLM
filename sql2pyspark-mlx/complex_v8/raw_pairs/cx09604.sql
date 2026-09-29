WITH base_moves AS (
    -- Scope the stock_moves rows
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
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY warehouse_id, move_type) AS rows_in_warehouse_id_move_type,
        COUNT(*) OVER (PARTITION BY operator, move_type) AS rows_in_operator_move_type,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type, operator), 2) AS unit_price_vs_move_type_operator_avg,
        MAX(unit_price) OVER (PARTITION BY sku_id, operator ORDER BY moved_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS unit_price_max_1_prior,
        ROUND(qty - AVG(qty) OVER (PARTITION BY move_type, operator), 2) AS qty_vs_move_type_operator_avg,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id, move_type), 2) AS qty_vs_warehouse_id_move_type_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at), 3) AS moved_at_percent_rank,
        RANK() OVER (ORDER BY moved_at) AS moved_at_rank,
        LAST_VALUE(moved_at) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at
    FROM base_moves
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE moved_at_rank <= 5
      AND operator = 'op_bee'
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    rows_in_warehouse_id_move_type,
    rows_in_operator_move_type,
    unit_price_vs_move_type_operator_avg,
    unit_price_max_1_prior,
    qty_vs_move_type_operator_avg,
    last_known_unit_price,
    qty_vs_warehouse_id_move_type_avg,
    moved_at_percent_rank,
    moved_at_rank,
    last_moved_at,
    SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
    moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m
FROM top_per_group
ORDER BY moved_at_plus_15m
