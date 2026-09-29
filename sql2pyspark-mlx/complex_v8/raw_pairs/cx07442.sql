SELECT id AS qty
FROM skus
EXCEPT
SELECT capacity
FROM warehouses
WHERE YEAR(opened_on) = 2024
ORDER BY qty
