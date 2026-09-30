SELECT id AS qty
FROM warehouses
WHERE opened_on < DATE '2025-02-14'
  AND city IN ('Kuala Lumpur', 'Bangkok')
INTERSECT
SELECT id AS qty
FROM skus
ORDER BY qty
