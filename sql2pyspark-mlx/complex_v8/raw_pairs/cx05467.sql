SELECT status, adjuster_id AS qty, claim_type
FROM claims
WHERE adjuster_id >= 4
  AND claimed_amount BETWEEN 6745.1 AND 46415.5
UNION ALL
SELECT holder_name, id, segment
FROM policyholders
ORDER BY status DESC
