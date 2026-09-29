SELECT claim_type AS label, reported_at AS happened_at
FROM claims
WHERE policy_id >= 4
UNION ALL
SELECT claim_type, reported_at
FROM claims
ORDER BY label
