SELECT id, wh_code AS name
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
MINUS
SELECT warehouse_id, move_type
FROM stock_moves
ORDER BY id DESC
