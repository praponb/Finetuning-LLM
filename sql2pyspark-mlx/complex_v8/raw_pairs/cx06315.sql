SELECT holder_id AS entity_id
FROM policies
MINUS
SELECT id AS entity_id
FROM policyholders
