WITH covered AS (
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
with_parent AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_claimed_amount_60d,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM covered s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    policy_premium,
    policy_end_date,
    DATEDIFF(DATE '2026-01-01', policy_end_date) AS days_since_policy_end_date
FROM with_parent
ORDER BY id DESC, policy_end_date DESC
LIMIT 5
