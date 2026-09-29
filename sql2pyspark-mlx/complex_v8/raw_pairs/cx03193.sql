SELECT claim_type, adjuster_id, reported_at AS happened_at
FROM claims
UNION
SELECT
    status AS claim_type,
    policy_id AS adjuster_id,
    reported_at AS happened_at
FROM claims
ORDER BY claim_type DESC, adjuster_id
