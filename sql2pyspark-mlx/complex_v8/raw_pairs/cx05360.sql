SELECT id, joined_on
FROM policyholders
UNION ALL
SELECT adjuster_id AS id, claim_date AS joined_on
FROM claims
ORDER BY joined_on DESC, id
