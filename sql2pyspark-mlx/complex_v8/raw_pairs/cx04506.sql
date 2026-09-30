SELECT holder_name AS label, risk_score AS metric
FROM policyholders
WHERE UPPER(TRIM(holder_name)) LIKE 'G%'
UNION ALL
SELECT status, claimed_amount
FROM claims
WHERE policy_id IS NOT NULL
ORDER BY metric, label
