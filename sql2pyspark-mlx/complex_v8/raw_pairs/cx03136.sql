WITH matched AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.status,
        s.premium,
        p.holder_id AS policy_holder_id
    FROM matched s
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    policy_id,
    claimed_amount,
    adjuster_id,
    status,
    premium,
    policy_holder_id,
    ABS(claimed_amount - 17585.2) AS claimed_amount_distance
FROM enriched_policy
ORDER BY premium NULLS LAST, policy_id DESC
