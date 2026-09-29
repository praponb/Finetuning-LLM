WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_parent AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.approved_amount,
        s.claim_type,
        s.product,
        s.premium,
        p.start_date AS policy_start_date
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.start_date >= DATE '2024-10-22'
),
product_stats AS (
    -- Median approved_amount per product
    SELECT product, PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount
    FROM with_parent
    GROUP BY product
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount,
        ROUND(s.approved_amount - g.median_approved_amount, 2) AS approved_amount_minus_median
    FROM with_parent s
    JOIN product_stats g ON g.product = s.product
    WHERE s.approved_amount >= g.median_approved_amount
)
SELECT
    id,
    policy_id,
    claim_date,
    approved_amount,
    claim_type,
    premium,
    policy_start_date,
    median_approved_amount,
    approved_amount_minus_median,
    median_approved_amount + premium AS median_approved_amount_plus_premium
FROM vs_group
WHERE median_approved_amount <= 30000.0
ORDER BY policy_id, claim_type, id
LIMIT 5
