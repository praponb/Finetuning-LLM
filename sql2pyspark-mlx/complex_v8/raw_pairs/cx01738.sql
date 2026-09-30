WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_parent_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.claim_type,
        s.product,
        s.premium,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date
    FROM matched s
    JOIN policies p ON p.id = s.policy_id
    WHERE YEAR(p.start_date) = 2024
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(premium, claimed_amount) AS least_premium_claimed_amount,
        DATEDIFF(policy_end_date, policy_start_date) AS days_policy_start_date_to_policy_end_date
    FROM with_parent_policy
    WHERE product <> 'AUTO'
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    status,
    claim_type,
    product,
    premium,
    least_premium_claimed_amount,
    days_policy_start_date_to_policy_end_date,
    DATE_TRUNC('QUARTER', policy_start_date) AS policy_start_date_quarter,
    ROUND(claimed_amount / NULLIF(premium, 0), 3) AS claimed_amount_per_premium
FROM labeled
ORDER BY policy_id DESC, id
LIMIT 10
