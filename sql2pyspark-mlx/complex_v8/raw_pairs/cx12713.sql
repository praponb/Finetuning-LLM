SELECT id AS entity_id, unit_price, qty AS key_id
FROM stock_moves
UNION
SELECT qty, unit_price, sku_id
FROM stock_moves
WHERE operator IN ('op_dew', 'op_bee', 'op_eve')
  AND sku_id >= 7
ORDER BY key_id DESC
