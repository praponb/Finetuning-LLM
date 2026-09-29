WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-03-08'
      AND warehouse_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty,
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM cleaned
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, unit_price, greatest_unit_price_qty
    FROM scored
) src
UNPIVOT (
    metric_value FOR metric_name IN (unit_price, greatest_unit_price_qty)
) unp
ORDER BY id, metric_name
