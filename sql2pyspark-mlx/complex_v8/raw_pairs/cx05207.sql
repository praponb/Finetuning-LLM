SELECT
    warehouse_id,
    operator AS name,
    qty,
    'move' AS src
FROM stock_moves
UNION ALL
SELECT
    sku_id,
    move_type,
    warehouse_id,
    'move_2' AS src
FROM stock_moves
UNION ALL
SELECT
    sku_id,
    operator,
    id,
    'move_3' AS src
FROM stock_moves
WHERE unit_price >= 119.5
  AND operator NOT IN ('op_ann', 'op_bee')
ORDER BY name
