SELECT id
FROM policyholders
EXCEPT
SELECT id
FROM policyholders
WHERE joined_on < DATE '2025-09-02'
  AND risk_score < 0.8
ORDER BY id
