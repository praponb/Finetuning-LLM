SELECT status AS label, reported_at AS happened_at
FROM claims
WHERE claim_type = 'collision'
UNION ALL
SELECT claim_type AS label, reported_at AS happened_at
FROM claims
UNION ALL
SELECT status AS label, reported_at AS happened_at
FROM claims
ORDER BY happened_at, label
