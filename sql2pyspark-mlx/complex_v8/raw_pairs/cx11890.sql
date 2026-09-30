WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.approved_amount,
        s.product,
        s.premium,
        p.status AS policy_status,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium
    FROM in_window s
    LEFT JOIN policies p ON p.id = s.policy_id
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', policy_end_date) AS policy_end_date_month,
        product,
        ROUND(AVG(approved_amount), 2) AS avg_approved_amount,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        ROUND(SUM(policy_premium), 2) AS total_policy_premium,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(premium) AS max_premium
    FROM enriched_policy
    GROUP BY DATE_TRUNC('MONTH', policy_end_date), product
    HAVING SUM(approved_amount) > 0.0
)
SELECT
    distinct_policy_ids,
    product,
    distinct_ids,
    COUNT(DISTINCT product) AS distinct_products
FROM per_group
GROUP BY GROUPING SETS ((distinct_policy_ids, product, distinct_ids), (distinct_policy_ids), ())
ORDER BY distinct_policy_ids NULLS LAST, product NULLS LAST, distinct_ids NULLS LAST
