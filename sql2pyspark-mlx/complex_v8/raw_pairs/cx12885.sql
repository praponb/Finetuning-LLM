SELECT
    'claim' AS record_type,
    adjuster_id,
    approved_amount AS amount,
    status
FROM claims
UNION ALL
SELECT
    'claim_2' AS record_type,
    id,
    approved_amount,
    status
FROM claims
WHERE claimed_amount <= 41878.5
  AND policy_id IS NOT NULL
ORDER BY amount DESC, record_type
