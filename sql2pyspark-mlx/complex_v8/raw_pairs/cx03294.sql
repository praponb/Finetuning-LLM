SELECT region AS name, id AS qty
FROM policyholders
WHERE joined_on IS NOT NULL
  AND segment = 'SME'
UNION ALL
SELECT claim_type AS name, id AS qty
FROM claims
ORDER BY qty
