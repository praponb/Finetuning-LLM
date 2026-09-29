WITH base_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT_WS(' / ', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE policy_id >= 12
      AND claim_date >= DATE '2024-04-14'
)
SELECT 'claim' AS origin, policy_id AS key_id, claimed_amount
FROM base_claims
WHERE policy_id <= 19
  AND HOUR(reported_at) BETWEEN 7 AND 10
UNION ALL
SELECT 'claim_2' AS origin, policy_id, claimed_amount
FROM claims
UNION ALL
SELECT 'holder' AS origin, id, risk_score
FROM policyholders
