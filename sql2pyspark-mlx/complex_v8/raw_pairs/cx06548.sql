WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE unit_price <= 289.7
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty_per_unit_price,
        ROUND(qty_per_unit_price * 1.5, 2) AS qty_per_unit_price_adj,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        ABS(days_since_moved_at - 490) AS days_since_moved_at_distance
    FROM filtered
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, qty_per_unit_price_adj, unit_price
    FROM labeled
) src
UNPIVOT (
    value FOR metric_name IN (qty_per_unit_price_adj AS m_qty_per_unit_price_adj, unit_price AS m_unit_price)
) unp
