SELECT holder_id
FROM policies
EXCEPT ALL
SELECT id
FROM policyholders
WHERE segment = 'SME'
  AND risk_score > 0.2
ORDER BY holder_id DESC
