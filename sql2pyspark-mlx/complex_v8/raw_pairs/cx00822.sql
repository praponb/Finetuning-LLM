SELECT capacity AS entity_id, wh_code
FROM warehouses
INTERSECT
SELECT capacity, city
FROM warehouses
WHERE opened_on < DATE '2025-02-14'
