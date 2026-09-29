SELECT claim_type AS name, claim_date AS event_date, reported_at AS happened_at
FROM claims
UNION
SELECT status AS name, claim_date AS event_date, reported_at AS happened_at
FROM claims
WHERE claim_date < DATE '2025-09-14'
