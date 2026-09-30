WITH cleaned_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE approved_amount <= 23487.5
)
SELECT 'claim' AS record_type, claim_type, claimed_amount
FROM cleaned_claims
UNION ALL
SELECT 'holder' AS record_type, region, risk_score
FROM policyholders
UNION ALL
SELECT 'claim_3' AS record_type, status, claimed_amount
FROM claims
WHERE adjuster_id <= 2
  AND policy_id <= 20
ORDER BY claim_type
