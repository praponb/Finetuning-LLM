WITH joined_ranges AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
qualified AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.start_date >= DATE '2024-04-03'
        )
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT MAX(c.claimed_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS max_claim_claimed_amount,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_approved_amount
    FROM qualified s
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(claim_date) AS last_claim_date,
        MAX(claim_date) AS last_claim_date_2,
        MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date,
        c.last_claim_date_2,
        c.max_claim_approved_amount
    FROM summarized_rows s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    status,
    approved_amount,
    product,
    premium,
    claim_count,
    max_claim_claimed_amount,
    last_claim_date_2,
    max_claim_approved_amount
FROM with_stats
ORDER BY claim_count DESC
