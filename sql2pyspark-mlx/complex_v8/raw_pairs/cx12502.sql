WITH in_window AS (
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
rollup_base AS (
    -- Group-level totals
    SELECT
        product,
        claim_type,
        ROUND(100 * COUNT(CASE WHEN product = 'HOME' THEN 1 END) / COUNT(*), 1) AS pct_product_home,
        COUNT(*) AS cnt,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        SUM(claimed_amount) AS total_claimed_amount
    FROM in_window
    WHERE claimed_amount <= 33690.0
    GROUP BY product, claim_type
    HAVING MAX(approved_amount) > 2569.2
)
-- Final projection
SELECT
    product,
    pct_product_home,
    cnt,
    distinct_policy_ids,
    total_claimed_amount
FROM rollup_base
ORDER BY pct_product_home DESC, total_claimed_amount
