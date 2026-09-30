WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.adjuster_id,
        s.status,
        s.product,
        s.premium,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount
    FROM covered s
    JOIN claims c ON c.policy_id = s.policy_id
),
combined_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.adjuster_id,
        s.status,
        s.product,
        s.premium,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_approved_amount,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM with_children s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
enriched_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium_2,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at
    FROM combined_details_adjuster s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN claims c ON c.id = s.claim_id
)
-- Final projection
SELECT
    id,
    claim_date,
    adjuster_id,
    status,
    product,
    premium,
    claim_id,
    claim_adjuster_id,
    adjuster_level,
    adjuster_team,
    policy_product,
    policy_holder_id,
    policy_premium_2,
    LEAST(claim_approved_amount, adjuster_level) AS least_claim_approved_amount_adjuster_level
FROM enriched_policy
ORDER BY claim_adjuster_id, id, claim_id
LIMIT 5
