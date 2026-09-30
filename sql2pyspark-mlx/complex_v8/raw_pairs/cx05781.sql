SELECT capacity AS entity_id, wh_code
FROM warehouses
EXCEPT
SELECT qty AS entity_id, move_type AS wh_code
FROM stock_moves
WHERE move_type IS NOT NULL
