WITH joined_ranges AS (
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
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MIN(claimed_amount) AS min_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_claimed_amount
    FROM joined_ranges s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(reported_at) AS first_claim_reported_at,
        SUM(approved_amount) AS total_claim_approved_amount,
        MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at,
        c.total_claim_approved_amount,
        c.last_claim_date
    FROM with_stats s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.policy_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT first_claim_reported_at, product, status, total_claim_approved_amount
    FROM with_counts
) src
PIVOT (
    SUM(total_claim_approved_amount) AS amount, COUNT(total_claim_approved_amount) AS n
    FOR status IN ('PAID', 'APPROVED', 'OPEN')
)
ORDER BY first_claim_reported_at, product
