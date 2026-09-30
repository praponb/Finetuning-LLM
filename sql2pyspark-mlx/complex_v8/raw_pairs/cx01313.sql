SELECT id, reported_at, claim_date
FROM claims
UNION
SELECT adjuster_id, reported_at, claim_date
FROM claims
