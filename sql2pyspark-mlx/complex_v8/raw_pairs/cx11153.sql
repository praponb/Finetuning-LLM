SELECT operator
FROM stock_moves
EXCEPT ALL
SELECT wh_code AS operator
FROM warehouses
WHERE capacity < 11768
  AND city <> 'Kuala Lumpur'
ORDER BY operator
