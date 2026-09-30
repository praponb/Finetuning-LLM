SELECT claim_type AS name, reported_at
FROM claims
WHERE TO_DATE(reported_at) < DATE '2025-02-18'
UNION ALL
SELECT status AS name, reported_at
FROM claims
WHERE approved_amount > 1273.7
ORDER BY reported_at, name
