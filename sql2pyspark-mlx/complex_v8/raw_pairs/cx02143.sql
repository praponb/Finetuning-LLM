SELECT id AS qty, reported_at AS happened_at
FROM claims
UNION ALL
SELECT policy_id, reported_at
FROM claims
