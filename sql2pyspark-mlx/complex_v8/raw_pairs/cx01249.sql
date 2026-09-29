SELECT id AS key_id, claim_date, adjuster_id
FROM claims
WHERE adjuster_id <= 5
  AND claimed_amount <= 29336.0
UNION ALL
SELECT adjuster_id AS key_id, claim_date, policy_id AS adjuster_id
FROM claims
WHERE policy_id <= 27
ORDER BY adjuster_id
