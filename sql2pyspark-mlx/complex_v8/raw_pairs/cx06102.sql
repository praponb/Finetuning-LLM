SELECT segment AS name, risk_score, 'holder' AS source
FROM policyholders
UNION ALL
SELECT segment, risk_score, 'holder_2' AS source
FROM policyholders
