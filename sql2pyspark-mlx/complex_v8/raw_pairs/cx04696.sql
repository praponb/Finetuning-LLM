WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT s.*, p.end_date AS policy_end_date
    FROM covered s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_parent AS (
    -- Join policies
    SELECT s.*, p.holder_id AS policy_holder_id, po.risk_score AS holder_risk_score
    FROM combined_details_policy s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
SELECT
    id,
    policy_id,
    approved_amount,
    claim_type,
    product,
    policy_end_date,
    policy_holder_id,
    holder_risk_score,
    LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
    DATE_TRUNC('MONTH', policy_end_date) AS policy_end_date_month
FROM with_parent
ORDER BY claim_type_slug DESC
