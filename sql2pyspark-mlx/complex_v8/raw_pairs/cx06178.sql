SELECT claim_type AS category, reported_at AS event_time, 'claim' AS source
FROM claims
WHERE claimed_amount > 19337.6
UNION ALL
SELECT claim_type, reported_at, 'claim_2' AS source
FROM claims
ORDER BY event_time DESC, category
