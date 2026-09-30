SELECT 'holder' AS record_type, id, risk_score
FROM policyholders
WHERE region <> 'East'
UNION ALL
SELECT 'holder_2' AS record_type, id, risk_score
FROM policyholders
WHERE joined_on >= DATE '2024-06-18'
  AND risk_score >= 0.1
ORDER BY risk_score, id
