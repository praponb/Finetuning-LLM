SELECT
    status AS name,
    reported_at AS happened_at,
    claim_type AS category,
    'claim' AS origin
FROM claims
WHERE claim_date IS NOT NULL
  AND claimed_amount >= 18778.6
UNION ALL
SELECT
    claim_type AS name,
    reported_at AS happened_at,
    status AS category,
    'claim_2' AS origin
FROM claims
ORDER BY happened_at, origin
