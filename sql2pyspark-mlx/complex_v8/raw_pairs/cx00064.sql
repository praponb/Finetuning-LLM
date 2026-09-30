WITH matched AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.claim_type,
        s.product,
        s.premium,
        p.premium AS policy_premium,
        po.joined_on AS holder_joined_on,
        po.holder_name
    FROM matched s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.policy_premium,
        s.holder_joined_on,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date AS claim_claim_date
    FROM enriched s
    JOIN claims c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    holder_joined_on,
    claim_id,
    claim_approved_amount,
    claim_claimed_amount,
    claim_claim_date,
    CASE WHEN claim_claim_type IN ('medical', 'fire') THEN 'group_a' WHEN claim_claim_type = 'delay' THEN 'group_b' ELSE 'other' END AS claim_claim_type_group
FROM detail_rows_claims
ORDER BY holder_joined_on NULLS LAST, claim_claimed_amount
