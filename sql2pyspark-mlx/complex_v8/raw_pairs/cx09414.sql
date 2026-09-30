SELECT id, reported_at, adjuster_id AS key_id
FROM claims
WHERE claim_type = 'theft'
  AND policy_id <= 17
UNION
SELECT policy_id, reported_at, id
FROM claims
WHERE status IN ('REJECTED', 'IN_REVIEW', 'APPROVED')
