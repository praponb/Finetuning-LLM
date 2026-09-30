SELECT status, reported_at, 'claim' AS src
FROM claims
WHERE claim_type = 'theft'
  AND TO_DATE(reported_at) < DATE '2025-03-14'
UNION ALL
SELECT claim_type, reported_at, 'claim_2' AS src
FROM claims
