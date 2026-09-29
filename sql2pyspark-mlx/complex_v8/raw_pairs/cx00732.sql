WITH prepared AS (
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
        GREATEST(unit_price, qty) AS greatest_unit_price_qty
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-19 09:00:00'
      AND move_type = 'INBOUND'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        greatest_unit_price_qty,
        COUNT(*) OVER w_operator_move_type AS rows_in_operator_move_type,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id ORDER BY greatest_unit_price_qty DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS qty_avg_7,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(moved_at), operator ORDER BY qty), 3) AS qty_cume_dist,
        qty - LAG(qty) OVER (PARTITION BY YEAR(moved_at), operator ORDER BY qty, id) AS qty_change
    FROM prepared
    WINDOW w_operator_move_type AS (PARTITION BY operator, move_type)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        qty,
        operator,
        unit_price,
        rows_in_operator_move_type,
        qty_avg_7,
        qty_cume_dist,
        NTH_VALUE(qty_avg_7, 2) OVER (PARTITION BY move_type, operator ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_avg_7,
        RANK() OVER (PARTITION BY move_type, operator ORDER BY unit_price DESC) AS unit_price_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type, MONTH(moved_at) ORDER BY greatest_unit_price_qty), 3) AS greatest_unit_price_qty_percent_rank,
        MIN(qty_change) OVER (PARTITION BY move_type, operator ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_change_so_far,
        DENSE_RANK() OVER (PARTITION BY operator, sku_id ORDER BY rows_in_operator_move_type DESC, id) AS rows_in_operator_move_type_dense_rank,
        ROUND(rows_in_operator_move_type - AVG(rows_in_operator_move_type) OVER (PARTITION BY sku_id, operator), 2) AS rows_in_operator_move_type_vs_sku_id_operator_av
    FROM metrics
)
SELECT *
FROM (
    SELECT rows_in_operator_move_type_dense_rank, operator, rows_in_operator_move_type_vs_sku_id_operator_av
    FROM ranked
) src
PIVOT (
    COUNT(rows_in_operator_move_type_vs_sku_id_operator_av)
    FOR operator IN ('op_ann', 'op_chan', 'op_bee', 'op_dew')
)
