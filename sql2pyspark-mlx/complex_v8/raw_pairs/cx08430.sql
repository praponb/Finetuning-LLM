WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.approved_amount) AS prior_approved_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_180d,
        s.prior_approved_amount_180d,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium,
        po.segment AS holder_segment,
        po.joined_on AS holder_joined_on
    FROM matched s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent_policy s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_date < DATE '2025-01-10'
              AND c.claim_type = 'collision'
        )
      AND s.holder_segment = 'SME'
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_180d,
    prior_approved_amount_180d,
    policy_start_date,
    policy_premium,
    holder_joined_on,
    ROUND(policy_premium * 0.25, 2) AS policy_premium_adj
FROM qualified
ORDER BY policy_id, prior_approved_amount_180d DESC, id
LIMIT 10
