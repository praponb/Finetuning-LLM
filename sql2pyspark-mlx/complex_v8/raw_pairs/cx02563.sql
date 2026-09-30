SELECT id AS qty
FROM policyholders
WHERE risk_score < 0.7
INTERSECT ALL
SELECT id AS qty
FROM policies
WHERE status IN ('ACTIVE', 'LAPSED', 'CANCELLED')
  AND premium < 3544.1
ORDER BY qty
