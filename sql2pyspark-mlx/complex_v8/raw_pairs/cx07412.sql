SELECT id AS qty
FROM warehouses
EXCEPT ALL
SELECT capacity AS qty
FROM warehouses
ORDER BY qty
