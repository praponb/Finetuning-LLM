SELECT
    claim_type,
    reported_at,
    policy_id AS qty,
    'claim' AS origin
FROM claims
WHERE status = 'OPEN'
  AND reported_at >= TIMESTAMP '2025-01-31 11:00:00'
UNION
SELECT
    status AS claim_type,
    reported_at,
    id AS qty,
    'claim_2' AS origin
FROM claims
WHERE adjuster_id IS NOT NULL
  AND claim_date IS NOT NULL
ORDER BY reported_at
