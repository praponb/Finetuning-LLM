SELECT
    id AS ref_id,
    claim_type,
    reported_at AS happened_at,
    'claim' AS record_type
FROM claims
WHERE claim_type IS NOT NULL
UNION ALL
SELECT
    id AS ref_id,
    status AS claim_type,
    reported_at AS happened_at,
    'claim_2' AS record_type
FROM claims
