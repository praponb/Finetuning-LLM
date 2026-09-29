SELECT policy_id AS entity_id, reported_at, approved_amount
FROM claims
WHERE claim_type <> 'fire'
  AND claim_date < DATE '2025-03-31'
UNION ALL
SELECT id AS entity_id, reported_at, approved_amount
FROM claims
WHERE reported_at >= TIMESTAMP '2025-01-19 14:00:00'
  AND claim_type NOT IN ('flood', 'fire')
