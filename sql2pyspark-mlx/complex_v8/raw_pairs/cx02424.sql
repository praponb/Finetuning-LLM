WITH joined_ranges AS (
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
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.claim_type,
        s.product,
        s.premium,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.claim_type AS claim_claim_type,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount
    FROM joined_ranges s
    JOIN claims c ON c.policy_id = s.policy_id AND c.claim_type <> 'collision'
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    product,
    premium,
    claim_claim_date,
    claim_status,
    claim_claimed_amount,
    CASE WHEN claim_claim_type IN ('medical', 'theft') THEN 'group_a' WHEN claim_claim_type = 'delay' THEN 'group_b' ELSE 'other' END AS claim_claim_type_group
FROM line_level
WHERE claim_type IS NOT NULL
