WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND unit_price >= 236.5
)
SELECT
    operator_or_unknown,
    id,
    move_type AS category,
    'move' AS origin
FROM cleaned_moves
WHERE moved_at >= TIMESTAMP '2025-01-31 10:00:00'
  AND warehouse_id <= 4
UNION
SELECT
    sku_code AS operator_or_unknown,
    id,
    category,
    'sku' AS origin
FROM skus
ORDER BY operator_or_unknown
