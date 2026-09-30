WITH cleaned AS (
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
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND unit_price < 396.4
)
SELECT warehouse_id AS qty, unit_price AS metric
FROM cleaned
UNION
SELECT sku_id, unit_price
FROM stock_moves
WHERE unit_price BETWEEN 160.8 AND 434.1
