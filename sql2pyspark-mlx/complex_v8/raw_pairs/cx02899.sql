SELECT id AS entity_id
FROM skus
EXCEPT
SELECT capacity AS entity_id
FROM warehouses
