SELECT id, wh_code
FROM warehouses
EXCEPT ALL
SELECT id, city
FROM warehouses
WHERE opened_on < DATE '2025-07-03'
  AND capacity >= 4912
ORDER BY id DESC
