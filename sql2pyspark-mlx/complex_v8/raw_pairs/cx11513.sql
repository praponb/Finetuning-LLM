SELECT holder_id
FROM policies
INTERSECT
SELECT adjuster_id AS holder_id
FROM claims
WHERE policy_id IS NOT NULL
