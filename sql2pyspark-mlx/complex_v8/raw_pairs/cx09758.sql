SELECT id AS key_id, holder_name
FROM policyholders
WHERE LOWER(holder_name) LIKE '%li%'
INTERSECT
SELECT id AS key_id, product AS holder_name
FROM policies
