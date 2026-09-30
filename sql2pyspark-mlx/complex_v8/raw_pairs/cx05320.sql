WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status
    FROM claims
    WHERE claim_date IS NOT NULL
)
SELECT 'claim' AS source, policy_id AS qty, approved_amount
FROM cleaned
UNION ALL
SELECT 'holder' AS source, id AS qty, risk_score AS approved_amount
FROM policyholders
WHERE UPPER(TRIM(holder_name)) LIKE 'E%'
  AND region IN ('Central', 'North')
