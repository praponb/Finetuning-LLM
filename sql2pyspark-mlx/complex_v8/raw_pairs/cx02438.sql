SELECT segment, risk_score AS metric, 'holder' AS origin
FROM policyholders
UNION
SELECT claim_type, approved_amount, 'claim' AS origin
FROM claims
