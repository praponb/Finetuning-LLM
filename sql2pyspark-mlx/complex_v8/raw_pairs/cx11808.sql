SELECT policy_id, status
FROM claims
EXCEPT ALL
SELECT id, region
FROM policyholders
WHERE region IN ('North', 'South', 'Central')
  AND segment = 'VIP'
ORDER BY policy_id, status
