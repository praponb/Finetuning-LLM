WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_60d,
        SUM(b.claimed_amount) AS prior_claimed_amount_60d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 60)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_parent_policy AS (
    -- Join policies
    SELECT s.*, p.premium AS policy_premium, p.start_date AS policy_start_date
    FROM joined_ranges s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    policy_premium,
    policy_start_date,
    DATE_ADD(policy_start_date, 365) AS policy_start_date_plus_365d,
    GREATEST(prior_claimed_amount_60d, prior_rows_60d) AS greatest_prior_claimed_amount_60d_prior_rows_60d
FROM with_parent_policy
ORDER BY policy_premium NULLS LAST
