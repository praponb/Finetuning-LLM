SELECT id AS ref_id
FROM policyholders
WHERE region IN ('Central', 'North', 'South')
EXCEPT ALL
SELECT level AS ref_id
FROM adjusters
WHERE level < 3
  AND team <> 'Travel'
ORDER BY ref_id
