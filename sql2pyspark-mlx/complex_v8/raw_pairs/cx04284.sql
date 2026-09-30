SELECT
    claim_type AS category,
    reported_at AS event_time,
    claimed_amount AS amount
FROM claims
WHERE adjuster_id IS NOT NULL
  AND approved_amount > 0.0
UNION ALL
SELECT claim_type, reported_at, approved_amount
FROM claims
