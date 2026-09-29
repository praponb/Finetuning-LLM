SELECT policy_id AS qty, reported_at AS event_time, status AS label
FROM claims
WHERE reported_at >= TIMESTAMP '2025-01-31 12:00:00'
UNION ALL
SELECT adjuster_id, reported_at, claim_type
FROM claims
WHERE claim_type = 'collision'
  AND claimed_amount <= 31752.9
ORDER BY qty
