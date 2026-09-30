WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.adjuster_id,
        s.product,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM covered s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.adjuster_id,
        s.policy_premium,
        s.policy_end_date,
        s.policy_holder_id,
        s.adjuster_level,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.approved_amount AS claim_approved_amount,
        c.policy_id AS claim_policy_id,
        c.claim_type
    FROM with_details_policy s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.policy_id >= 17
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    policy_premium,
    policy_holder_id,
    adjuster_level,
    claim_id,
    claim_approved_amount,
    claim_type,
    ROUND(policy_premium / NULLIF(adjuster_level, 0), 3) AS policy_premium_per_adjuster_level,
    COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
FROM with_children
WHERE policy_id IS NOT NULL
ORDER BY adjuster_id NULLS LAST, policy_premium_per_adjuster_level DESC, id, claim_id
LIMIT 20
