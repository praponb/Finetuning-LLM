SELECT policy_id, approved_amount AS amount
FROM claims
WHERE status IN ('REJECTED', 'OPEN', 'APPROVED')
  AND claim_date < DATE '2025-01-04'
UNION ALL
SELECT id AS policy_id, premium AS amount
FROM policies
WHERE premium >= 1541.8
UNION ALL
SELECT holder_id AS policy_id, premium AS amount
FROM policies
ORDER BY policy_id
