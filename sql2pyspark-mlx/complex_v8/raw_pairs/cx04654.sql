SELECT status, reported_at
FROM claims
WHERE claim_type = 'theft'
  AND YEAR(claim_date) = 2024
UNION ALL
SELECT claim_type AS status, reported_at
FROM claims
ORDER BY status DESC, reported_at
