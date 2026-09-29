WITH in_window AS (
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
    LEFT JOIN policies p
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
        s.status,
        s.adjuster_id,
        s.product,
        p.start_date AS policy_start_date
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    status,
    adjuster_id,
    policy_start_date,
    FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
FROM enriched_policy
WHERE status = 'APPROVED'
ORDER BY claimed_amount_bucket DESC, id
LIMIT 20
