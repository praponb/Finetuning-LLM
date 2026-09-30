SELECT sku_id AS entity_id, operator AS label
FROM stock_moves
WHERE move_type IN ('ADJUST', 'TRANSFER', 'OUTBOUND')
MINUS
SELECT id, sku_code
FROM skus
WHERE weight_kg < 21.0
ORDER BY entity_id, label
