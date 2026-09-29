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
product_stats AS (
    -- Max claimed_amount per product
    SELECT product, MAX(claimed_amount) AS max_claimed_amount
    FROM covered
    GROUP BY product
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claimed_amount,
        ROUND(s.claimed_amount - g.max_claimed_amount, 2) AS claimed_amount_minus_max
    FROM covered s
    JOIN product_stats g ON g.product = s.product
    WHERE s.claimed_amount = g.max_claimed_amount
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id >= 3
        )
      AND s.claimed_amount_minus_max <= 25000.0
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    approved_amount,
    product,
    premium,
    max_claimed_amount
FROM qualified
ORDER BY claim_date DESC, policy_id DESC
