SELECT id, city AS name, capacity AS key_id
FROM warehouses
UNION
SELECT sku_id AS id, move_type AS name, id AS key_id
FROM stock_moves
WHERE move_type IS NOT NULL
ORDER BY key_id DESC, id
