WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM covered
    WHERE claimed_amount < (SELECT MAX(claimed_amount) FROM covered)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    approved_amount,
    product,
    premium
FROM with_activity
ORDER BY premium DESC, policy_id, id
LIMIT 5
