SELECT capacity, opened_on
FROM warehouses
WHERE wh_code IS NOT NULL
  AND opened_on IS NOT NULL
UNION ALL
SELECT id, opened_on
FROM warehouses
ORDER BY opened_on
