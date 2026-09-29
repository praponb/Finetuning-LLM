SELECT id AS ref_id, claim_type AS label, 'claim' AS source
FROM claims
WHERE policy_id >= 10
UNION
SELECT policy_id AS ref_id, status AS label, 'claim_2' AS source
FROM claims
WHERE claimed_amount < 38909.4
  AND status <> 'OPEN'
