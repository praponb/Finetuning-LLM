SELECT capacity
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
  AND opened_on < DATE '2025-07-03'
EXCEPT
SELECT qty
FROM stock_moves
ORDER BY capacity
