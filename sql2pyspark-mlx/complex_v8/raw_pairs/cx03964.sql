SELECT policy_id AS key_id, reported_at
FROM claims
WHERE approved_amount >= 0.0
  AND adjuster_id IS NOT NULL
UNION ALL
SELECT policy_id, reported_at
FROM claims
UNION ALL
SELECT policy_id, reported_at
FROM claims
ORDER BY reported_at
