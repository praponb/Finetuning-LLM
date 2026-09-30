WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        product,
        premium,
        LEAST(premium, claimed_amount) AS least_premium_claimed_amount,
        GREATEST(claimed_amount, premium) AS greatest_claimed_amount_premium
    FROM covered
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claim_type,
        s.product,
        s.premium,
        s.least_premium_claimed_amount,
        p.product AS policy_product,
        p.status AS policy_status,
        p.start_date AS policy_start_date
    FROM enriched s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.status = 'CANCELLED'
)
-- Final projection
SELECT
    id,
    claim_date,
    claim_type,
    product,
    least_premium_claimed_amount,
    policy_product,
    policy_status,
    ROUND(premium * 0.25, 2) AS premium_adj,
    ADD_MONTHS(policy_start_date, 3) AS policy_start_date_plus_3m
FROM combined_details
ORDER BY premium_adj DESC
