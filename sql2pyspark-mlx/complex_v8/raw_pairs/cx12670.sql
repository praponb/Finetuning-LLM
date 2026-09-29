WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.adjuster_id,
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
        s.approved_amount,
        s.adjuster_id,
        s.product,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date AS claim_claim_date
    FROM covered s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
claim_status_stats AS (
    -- Avg approved_amount per claim_status
    SELECT claim_status, AVG(approved_amount) AS avg_approved_amount
    FROM with_children
    GROUP BY claim_status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM with_children s
    JOIN claim_status_stats g ON g.claim_status = s.claim_status
    WHERE s.approved_amount > g.avg_approved_amount
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    product,
    claim_id,
    claim_reported_at,
    claim_claimed_amount,
    claim_claim_date,
    avg_approved_amount,
    CONCAT_WS('|', claim_status, product) AS claim_status_product_label
FROM above_average
ORDER BY claim_claimed_amount DESC
