SELECT status, reported_at AS event_time, id AS entity_id
FROM claims
WHERE claim_type NOT IN ('flood', 'medical')
  AND claim_date < DATE '2024-12-05'
UNION ALL
SELECT claim_type, reported_at, adjuster_id
FROM claims
ORDER BY status
