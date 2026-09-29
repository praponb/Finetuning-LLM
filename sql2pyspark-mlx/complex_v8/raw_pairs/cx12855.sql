SELECT policy_id, reported_at AS happened_at, status
FROM claims
UNION ALL
SELECT policy_id, reported_at, status
FROM claims
