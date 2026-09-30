SELECT id AS qty, region AS category, 'holder' AS origin
FROM policyholders
WHERE joined_on >= DATE '2024-10-23'
  AND segment NOT IN ('VIP', 'SME')
UNION
SELECT id AS qty, status AS category, 'claim' AS origin
FROM claims
