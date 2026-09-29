WITH recent_moves AS (
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
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-03-08'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        move_type_len,
        SUM(unit_price) OVER (PARTITION BY move_type_len, move_type ORDER BY move_type_len DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        MAX(qty) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_qty,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY move_type_len, move_type ORDER BY move_type_len DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        ROUND(AVG(move_type_len) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_move_type_len
    FROM recent_moves
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        moved_at,
        operator,
        move_type_len,
        running_unit_price,
        last_known_unit_price,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj,
        LOWER(TRIM(operator)) AS operator_clean,
        TO_DATE(moved_at) AS moved_at_date
    FROM metrics
)
SELECT *
FROM (
    SELECT moved_at, operator, unit_price_adj
    FROM annotated
) src
PIVOT (
    SUM(unit_price_adj)
    FOR operator IN ('op_dew', 'op_ann', 'op_bee')
)
ORDER BY moved_at
