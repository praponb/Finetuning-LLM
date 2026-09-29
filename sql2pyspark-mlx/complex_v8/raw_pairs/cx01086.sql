WITH base AS (
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
    WHERE qty <= 387
),
stats AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        COUNT(*) AS n_rows,
        ROUND(100 * COUNT(CASE WHEN operator = 'op_bee' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_bee,
        ROUND(SUM(unit_price) / NULLIF(SUM(qty), 0), 3) AS unit_price_to_qty_ratio,
        MIN(moved_at) AS first_moved_at,
        COUNT(*) FILTER (WHERE move_type = 'ADJUST') AS n_move_type_adjust
    FROM base
    WHERE unit_price > 164.1
    GROUP BY warehouse_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT warehouse_id, field, metric_value
FROM (
    SELECT warehouse_id, pct_operator_op_bee, unit_price_to_qty_ratio
    FROM stats
) src
UNPIVOT (
    metric_value FOR field IN (pct_operator_op_bee AS pct_operator_op_bee_v, unit_price_to_qty_ratio AS m_unit_price_to_qty_ratio)
) unp
