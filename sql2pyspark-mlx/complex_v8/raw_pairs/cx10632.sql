WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        move_type,
        operator,
        COUNT(*) OVER (PARTITION BY warehouse_id, sku_id) AS rows_in_warehouse_id_sku_id,
        SUM(qty) OVER (PARTITION BY move_type ORDER BY qty DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS qty_sum_1_prior,
        ROUND(PERCENT_RANK() OVER w_sku_id_move_type_by_unit_pri, 3) AS unit_price_percent_rank,
        FIRST_VALUE(qty) OVER (PARTITION BY move_type ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        qty - LAG(qty) OVER w_move_type_by_qty AS qty_change
    FROM base_moves
    WINDOW w_sku_id_move_type_by_unit_pri AS (PARTITION BY sku_id, move_type ORDER BY unit_price DESC), w_move_type_by_qty AS (PARTITION BY move_type ORDER BY qty DESC, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        move_type,
        operator,
        rows_in_warehouse_id_sku_id,
        qty_sum_1_prior,
        first_qty,
        qty_change,
        ABS(qty_change - -200) AS qty_change_distance,
        CONCAT_WS('-', move_type, operator) AS move_type_operator_label,
        UPPER(TRIM(operator)) AS operator_clean
    FROM sequenced
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT operator, move_type, qty_sum_1_prior
    FROM derived
) src
PIVOT (
    MIN(qty_sum_1_prior)
    FOR move_type IN ('INBOUND', 'TRANSFER', 'OUTBOUND', 'ADJUST')
)
