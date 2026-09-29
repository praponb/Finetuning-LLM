SELECT policy_id, claim_type AS name
FROM claims
INTERSECT
SELECT holder_id AS policy_id, status AS name
FROM policies
ORDER BY name
