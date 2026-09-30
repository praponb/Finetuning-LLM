SELECT
    policy_id AS ref_id,
    id AS entity_id,
    status,
    'claim' AS origin
FROM claims
UNION ALL
SELECT
    policy_id,
    adjuster_id,
    status,
    'claim_2' AS origin
FROM claims
ORDER BY status
