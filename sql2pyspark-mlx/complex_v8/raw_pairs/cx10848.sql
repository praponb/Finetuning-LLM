SELECT id
FROM warehouses
WHERE LENGTH(TRIM(wh_code)) >= 7
EXCEPT
SELECT qty
FROM stock_moves
ORDER BY id
