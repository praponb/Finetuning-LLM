SELECT policy_id, claim_date, claim_type
FROM claims
WHERE status IN ('PAID', 'REJECTED', 'APPROVED')
  AND approved_amount >= 0.0
UNION
SELECT id, claim_date, status
FROM claims
WHERE policy_id IS NOT NULL
ORDER BY policy_id, claim_type
