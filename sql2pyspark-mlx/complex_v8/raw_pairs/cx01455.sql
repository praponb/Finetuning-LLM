SELECT 'claim' AS origin, claim_type, claim_date AS event_date
FROM claims
WHERE adjuster_id <= 4
  AND status = 'APPROVED'
UNION ALL
SELECT 'claim_2' AS origin, claim_type, claim_date
FROM claims
WHERE adjuster_id IS NOT NULL
ORDER BY claim_type
