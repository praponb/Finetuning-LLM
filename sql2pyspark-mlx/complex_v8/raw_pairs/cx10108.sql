SELECT id AS entity_id
FROM policies
INTERSECT
SELECT id
FROM policyholders
ORDER BY entity_id
