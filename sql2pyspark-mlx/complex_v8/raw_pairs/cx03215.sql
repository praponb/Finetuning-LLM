SELECT wh_code
FROM warehouses
WHERE YEAR(opened_on) = 2025
  AND city <> 'Bangkok'
INTERSECT ALL
SELECT operator
FROM stock_moves
WHERE unit_price BETWEEN 37.6 AND 323.9
  AND sku_id IS NOT NULL
ORDER BY wh_code DESC
