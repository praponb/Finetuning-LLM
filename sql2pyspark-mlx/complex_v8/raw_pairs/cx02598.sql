SELECT capacity
FROM warehouses
WHERE opened_on >= DATE '2024-02-19'
EXCEPT
SELECT id
FROM warehouses
WHERE city <> 'Bangkok'
ORDER BY capacity
