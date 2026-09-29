WITH in_window AS (
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
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_id,
        status,
        adjuster_id,
        MAX(claimed_amount) AS max_claimed_amount,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        ROUND(SUM(premium) / NULLIF(SUM(claimed_amount), 0), 3) AS premium_to_claimed_amount_ratio,
        COUNT(DISTINCT status) AS distinct_status,
        SUM(CASE WHEN product = 'TRAVEL' THEN 1 ELSE 0 END) AS n_product_travel,
        ROUND(STDDEV(premium), 2) AS stddev_premium
    FROM in_window
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    policy_id,
    status,
    max_claimed_amount,
    distinct_policy_ids,
    premium_to_claimed_amount_ratio,
    distinct_status,
    n_product_travel,
    stddev_premium
FROM totals
ORDER BY status DESC, premium_to_claimed_amount_ratio DESC
