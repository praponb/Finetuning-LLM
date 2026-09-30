WITH matched AS (
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
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.policy_id IS NOT NULL
              AND c.claim_date BETWEEN DATE '2024-03-16' AND DATE '2025-11-08'
        )
)
SELECT
    COALESCE(product, 'ALL') AS product,
    policy_id,
    COUNT(*) FILTER (WHERE product <> 'TRAVEL') AS n_product_travel,
    COUNT(*) AS row_count
FROM qualified
GROUP BY CUBE(product, policy_id)
ORDER BY product NULLS LAST, policy_id NULLS LAST
