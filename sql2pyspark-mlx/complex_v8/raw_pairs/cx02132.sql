SELECT policy_id AS qty, reported_at AS happened_at
FROM claims
UNION ALL
SELECT id AS qty, reported_at AS happened_at
FROM claims
ORDER BY happened_at, qty
