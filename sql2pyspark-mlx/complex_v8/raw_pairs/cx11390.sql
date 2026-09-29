WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.status,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.premium,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount
    FROM joined_ranges s
    JOIN claims c ON c.policy_id = s.policy_id
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_children s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claimed_amount <= 42447.2
              AND c.claim_type IS NOT NULL
        )
      AND s.claim_claimed_amount > 19337.6
)
-- Subtotals with ROLLUP
SELECT
    claim_id,
    claim_status,
    policy_id,
    MIN(claim_date) AS first_claim_date,
    DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
    COUNT(*) AS row_count
FROM qualified
GROUP BY ROLLUP(claim_id, claim_status, policy_id)
