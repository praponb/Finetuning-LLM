SELECT status, reported_at, adjuster_id AS key_id
FROM claims
WHERE reported_at < TIMESTAMP '2025-02-24 11:00:00'
  AND claimed_amount < 42838.9
UNION ALL
SELECT claim_type, reported_at, id
FROM claims
ORDER BY reported_at DESC
