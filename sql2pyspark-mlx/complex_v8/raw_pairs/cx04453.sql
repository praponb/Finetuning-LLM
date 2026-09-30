WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        UPPER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-31 10:00:00'
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared_moves
    WHERE unit_price < (SELECT MAX(unit_price) FROM prepared_moves)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        move_type,
        ROUND(SUM(unit_price), 2) AS total_unit_price,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        SUM(CASE WHEN operator IN ('op_ann', 'op_eve', 'op_dew') THEN unit_price ELSE 0 END) AS unit_price_operator_in_op_ann_op_eve_op,
        COUNT(*) AS row_count
    FROM screened
    WHERE sku_id >= 18
    GROUP BY move_type
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT row_count, distinct_move_types, move_type, unit_price_operator_in_op_ann_op_eve_op
    FROM rollup_base
) src
PIVOT (
    AVG(unit_price_operator_in_op_ann_op_eve_op) AS avg_v, SUM(unit_price_operator_in_op_ann_op_eve_op) AS sum_v
    FOR move_type IN ('ADJUST', 'INBOUND', 'OUTBOUND')
)
ORDER BY row_count, distinct_move_types
