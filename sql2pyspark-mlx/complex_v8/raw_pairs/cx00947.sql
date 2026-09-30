SELECT adjuster_id AS key_id, reported_at AS happened_at
FROM claims
WHERE MONTH(reported_at) IN (2, 3)
  AND claimed_amount <= 43500.4
UNION ALL
SELECT policy_id AS key_id, reported_at AS happened_at
FROM claims
WHERE status = 'APPROVED'
  AND claimed_amount <= 42447.2
ORDER BY key_id DESC, happened_at
