WITH cleaned AS (
    -- Filter stock_moves
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
    WHERE (operator = 'op_bee' OR HOUR(moved_at) BETWEEN 6 AND 9)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price), 3) AS unit_price_percent_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at)), 2) AS unit_price_vs_sku_id_moved_at_month_avg,
        LAST_VALUE(moved_at) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        LEAD(unit_price, 3) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price NULLS LAST, id) AS next3_unit_price,
        MIN(unit_price) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at)) AS sku_id_moved_at_month_min_unit_price,
        SIZE(COLLECT_SET(move_type) OVER w_operator_warehouse_id) AS distinct_move_type_in_operator_warehouse_id,
        FIRST_VALUE(unit_price) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        ROUND(AVG(unit_price) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price NULLS LAST) AS unit_price_rank
    FROM cleaned
    WINDOW w_operator_warehouse_id AS (PARTITION BY operator, warehouse_id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_avg_3_prior,
        unit_price_percent_rank,
        unit_price_vs_sku_id_moved_at_month_avg,
        last_moved_at,
        next3_unit_price,
        sku_id_moved_at_month_min_unit_price,
        distinct_move_type_in_operator_warehouse_id,
        first_unit_price,
        unit_price_avg_7,
        last_known_moved_at
    FROM with_trends
    WHERE unit_price_rank = 1
)
SELECT
    id,
    moved_at,
    operator,
    unit_price,
    unit_price_avg_3_prior,
    next3_unit_price,
    distinct_move_type_in_operator_warehouse_id,
    last_known_moved_at,
    CONCAT_WS(': ', operator, move_type) AS operator_move_type_label,
    LEAST(qty, unit_price_avg_3_prior) AS least_qty_unit_price_avg_3_prior
FROM top_rows
ORDER BY moved_at
