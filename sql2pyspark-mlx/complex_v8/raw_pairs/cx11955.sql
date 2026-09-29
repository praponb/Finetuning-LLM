SELECT id, city, capacity AS qty
FROM warehouses
WHERE city NOT IN ('Kuala Lumpur', 'Bangkok')
  AND LOWER(wh_code) LIKE '%ku%'
UNION
SELECT capacity AS id, city, id AS qty
FROM warehouses
