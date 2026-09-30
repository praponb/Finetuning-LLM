SELECT city
FROM warehouses
EXCEPT ALL
SELECT sku_code
FROM skus
WHERE category IN ('Apparel', 'Beauty')
  AND unit_cost < 302.5
