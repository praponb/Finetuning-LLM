SELECT claim_type AS label, reported_at
FROM claims
WHERE approved_amount <= 15477.7
UNION ALL
SELECT status, reported_at
FROM claims
WHERE status = 'IN_REVIEW'
  AND reported_at < TIMESTAMP '2025-02-12 14:00:00'
UNION ALL
SELECT status, reported_at
FROM claims
