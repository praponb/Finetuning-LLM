SELECT claim_type, reported_at
FROM claims
UNION ALL
SELECT claim_type, reported_at
FROM claims
WHERE status IN ('PAID', 'REJECTED')
  AND adjuster_id IS NOT NULL
