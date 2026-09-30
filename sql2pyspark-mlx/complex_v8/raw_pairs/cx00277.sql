WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.claimed_amount) AS prior_claimed_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
joined_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_180d,
        s.prior_claimed_amount_180d,
        p.status AS policy_status,
        p.product AS policy_product,
        p.start_date AS policy_start_date
    FROM covered s
    JOIN policies p ON p.id = s.policy_id AND p.status IN ('CANCELLED', 'EXPIRED')
    WHERE p.product = 'HEALTH'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, policy_start_date, policy_status, prior_rows_180d
    FROM joined_policy
) src
PIVOT (
    MIN(prior_rows_180d)
    FOR policy_status IN ('ACTIVE', 'EXPIRED')
)
