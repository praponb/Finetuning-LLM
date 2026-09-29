SELECT category AS label, id AS ref_id
FROM skus
WHERE LOWER(sku_code) LIKE '%-4%'
  AND unit_cost BETWEEN 38.3 AND 330.6
UNION
SELECT wh_code, capacity
FROM warehouses
ORDER BY ref_id
