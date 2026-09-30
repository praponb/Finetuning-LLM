WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id <= 4
        )
      AND s.policy_id IS NOT NULL
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    adjuster_id,
    product,
    premium,
    ABS(claimed_amount - 28435.3) AS claimed_amount_distance,
    LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
FROM qualified
ORDER BY policy_id DESC
