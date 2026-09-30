WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        premium,
        LAG(approved_amount, 1, 0) OVER (ORDER BY premium, id) AS prev_approved_amount,
        LAG(claim_date) OVER (ORDER BY premium, id) AS prev_claim_date,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_all_by_premium) AS days_since_prev_claim_date
    FROM matched
    WINDOW w_all_by_premium AS (ORDER BY premium, id)
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_trends s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.approved_amount <= 14668.0
              AND c.adjuster_id IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    prev_approved_amount,
    prev_claim_date,
    days_since_prev_claim_date,
    ROUND(premium * 1.5, 2) AS premium_adj
FROM qualified
ORDER BY claim_date, premium_adj
