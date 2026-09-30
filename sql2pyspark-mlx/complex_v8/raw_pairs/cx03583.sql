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
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_id,
        ROUND(100 * COUNT(CASE WHEN product NOT IN ('HEALTH', 'HOME') THEN 1 END) / COUNT(*), 1) AS pct_product_not_in_health_home,
        ROUND(STDDEV(premium), 2) AS stddev_premium,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        ROUND(SUM(premium) / NULLIF(SUM(approved_amount), 0), 3) AS premium_to_approved_amount_ratio
    FROM matched
    GROUP BY policy_id
    HAVING AVG(claimed_amount) > 6745.1
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.end_date AS policy_end_date,
        p.status AS policy_status,
        po.region AS holder_region
    FROM summary s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
-- Final projection
SELECT
    policy_id,
    pct_product_not_in_health_home,
    claim_date_span_days,
    policy_status,
    holder_region
FROM with_parent
ORDER BY pct_product_not_in_health_home
