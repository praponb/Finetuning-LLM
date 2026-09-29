SELECT
    id,
    reported_at AS happened_at,
    claim_date AS event_date,
    'claim' AS record_type
FROM claims
UNION ALL
SELECT
    id,
    reported_at,
    claim_date,
    'claim_2' AS record_type
FROM claims
WHERE claim_type <> 'flood'
  AND claim_date < DATE '2025-08-25'
UNION ALL
SELECT
    policy_id,
    reported_at,
    claim_date,
    'claim_3' AS record_type
FROM claims
WHERE claim_type = 'collision'
ORDER BY happened_at, event_date
