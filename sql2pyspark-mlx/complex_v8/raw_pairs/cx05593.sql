WITH in_window AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        policy_id,
        adjuster_id,
        SUM(premium) AS total_premium,
        COUNT(*) AS row_count,
        ROUND(STDDEV(premium), 2) AS stddev_premium,
        ROUND(100 * COUNT(CASE WHEN product = 'HOME' THEN 1 END) / COUNT(*), 1) AS pct_product_home
    FROM in_window
    GROUP BY 1, 2, 3
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT row_count, adjuster_id, product, total_premium
    FROM totals
) src
PIVOT (
    MIN(total_premium)
    FOR product IN ('AUTO', 'LIFE', 'HOME')
)
