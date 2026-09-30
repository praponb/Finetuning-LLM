SELECT sku_code AS category
FROM skus
WHERE unit_cost >= 134.3
EXCEPT
SELECT wh_code AS category
FROM warehouses
WHERE opened_on < DATE '2025-07-03'
ORDER BY category DESC
