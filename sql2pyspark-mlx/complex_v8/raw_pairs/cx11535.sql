SELECT id
FROM policyholders
WHERE region IN ('East', 'North', 'South')
  AND segment IN ('RETAIL', 'VIP')
EXCEPT
SELECT adjuster_id
FROM claims
ORDER BY id
