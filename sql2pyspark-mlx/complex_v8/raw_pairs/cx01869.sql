SELECT id AS entity_id, risk_score
FROM policyholders
UNION ALL
SELECT id, risk_score
FROM policyholders
WHERE risk_score >= 0.3
ORDER BY risk_score
