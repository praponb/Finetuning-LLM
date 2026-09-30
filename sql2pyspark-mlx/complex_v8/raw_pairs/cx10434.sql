WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
policy_id_stats AS (
    -- Avg claimed_amount per policy_id
    SELECT policy_id, AVG(claimed_amount) AS avg_claimed_amount
    FROM joined_ranges
    GROUP BY policy_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM joined_ranges s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.claimed_amount > g.avg_claimed_amount
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    product,
    avg_claimed_amount,
    claimed_amount_minus_avg,
    COALESCE(premium, 0.0) AS premium_filled
FROM outliers
ORDER BY premium_filled, claim_date DESC
