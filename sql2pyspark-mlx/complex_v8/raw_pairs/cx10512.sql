WITH filtered_moves AS (
    -- Filter stock_moves
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
    WHERE unit_price > 160.8
)
SELECT move_type, unit_price AS amount, 'move' AS record_type
FROM filtered_moves
WHERE move_type IS NOT NULL
  AND operator IN ('op_dew', 'op_bee')
UNION ALL
SELECT category, weight_kg, 'sku' AS record_type
FROM skus
WHERE LOWER(sku_code) LIKE '%l-%'
UNION ALL
SELECT move_type, unit_price, 'move_3' AS record_type
FROM stock_moves
WHERE move_type IS NOT NULL
ORDER BY record_type
