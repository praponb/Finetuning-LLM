WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-18 15:00:00'
)
SELECT claim_type_or_unknown AS name, claimed_amount
FROM valid_claims
UNION
SELECT segment AS name, risk_score AS claimed_amount
FROM policyholders
ORDER BY name
