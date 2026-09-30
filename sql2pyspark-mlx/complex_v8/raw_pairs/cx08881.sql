WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty
    FROM valid
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, unit_price, greatest_unit_price_qty
    FROM labeled
) src
UNPIVOT (
    metric_value FOR metric_name IN (unit_price, greatest_unit_price_qty)
) unp
ORDER BY id, metric_name
