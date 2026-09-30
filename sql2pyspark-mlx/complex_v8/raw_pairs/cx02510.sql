SELECT
    claim_type,
    reported_at AS happened_at,
    approved_amount AS value,
    'claim' AS src
FROM claims
WHERE claim_date < DATE '2025-03-31'
  AND claimed_amount > 5774.6
UNION ALL
SELECT
    claim_type,
    reported_at,
    approved_amount,
    'claim_2' AS src
FROM claims
ORDER BY claim_type
