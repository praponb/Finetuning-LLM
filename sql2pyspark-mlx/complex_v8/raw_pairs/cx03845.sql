SELECT 'claim' AS record_type, policy_id, reported_at AS event_time
FROM claims
WHERE HOUR(reported_at) BETWEEN 9 AND 11
  AND claimed_amount >= 4868.7
UNION ALL
SELECT
    'claim_2' AS record_type,
    adjuster_id AS policy_id,
    reported_at AS event_time
FROM claims
WHERE claim_type = 'fire'
  AND status IS NOT NULL
ORDER BY policy_id, record_type
