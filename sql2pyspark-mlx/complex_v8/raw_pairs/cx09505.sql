SELECT claim_type, claimed_amount AS metric, 'claim' AS record_type
FROM claims
UNION ALL
SELECT claim_type, approved_amount, 'claim_2' AS record_type
FROM claims
WHERE claim_type <> 'theft'
  AND policy_id <= 18
ORDER BY claim_type, record_type
