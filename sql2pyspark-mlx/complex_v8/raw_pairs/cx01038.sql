SELECT status AS name, reported_at AS event_time, claimed_amount
FROM claims
UNION ALL
SELECT status, reported_at, claimed_amount
FROM claims
UNION ALL
SELECT status, reported_at, claimed_amount
FROM claims
WHERE status IS NOT NULL
ORDER BY event_time DESC
