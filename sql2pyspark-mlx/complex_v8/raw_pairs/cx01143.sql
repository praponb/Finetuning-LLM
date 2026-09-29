SELECT
    'claim' AS record_type,
    status AS label,
    claimed_amount,
    reported_at
FROM claims
WHERE claim_date BETWEEN DATE '2024-02-29' AND DATE '2025-08-10'
  AND policy_id <= 20
UNION ALL
SELECT
    'claim_2' AS record_type,
    status,
    approved_amount,
    reported_at
FROM claims
