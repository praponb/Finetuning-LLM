SELECT claim_type, reported_at, id
FROM claims
UNION ALL
SELECT status, reported_at, id
FROM claims
ORDER BY claim_type DESC
