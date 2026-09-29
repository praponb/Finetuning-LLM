SELECT status, claim_date, reported_at
FROM claims
UNION
SELECT claim_type AS status, claim_date, reported_at
FROM claims
ORDER BY status DESC
