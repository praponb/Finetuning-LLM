SELECT level, adjuster_name
FROM adjusters
INTERSECT ALL
SELECT policy_id AS level, claim_type AS adjuster_name
FROM claims
WHERE reported_at >= TIMESTAMP '2025-01-25 08:00:00'
