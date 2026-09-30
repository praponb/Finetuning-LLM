SELECT id AS key_id, adjuster_id AS key_id_2, claim_date
FROM claims
UNION ALL
SELECT adjuster_id, id, claim_date
FROM claims
ORDER BY key_id, claim_date
