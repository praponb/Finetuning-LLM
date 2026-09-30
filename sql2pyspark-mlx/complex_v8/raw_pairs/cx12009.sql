SELECT
    status,
    reported_at AS event_time,
    claim_type,
    'claim' AS record_type
FROM claims
UNION ALL
SELECT
    claim_type,
    reported_at,
    status,
    'claim_2' AS record_type
FROM claims
