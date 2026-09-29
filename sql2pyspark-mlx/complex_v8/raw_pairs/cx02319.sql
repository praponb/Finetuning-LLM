SELECT adjuster_id AS key_id, claim_date AS day, 'claim' AS origin
FROM claims
WHERE claim_date < DATE '2024-08-20'
  AND policy_id >= 21
UNION
SELECT id AS key_id, joined_on AS day, 'holder' AS origin
FROM policyholders
