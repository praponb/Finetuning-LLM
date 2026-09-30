SELECT id AS entity_id, reported_at, status
FROM claims
UNION
SELECT policy_id, reported_at, status
FROM claims
ORDER BY status DESC
