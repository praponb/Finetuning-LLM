WITH in_window AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        status,
        premium,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY status, claim_type)) AS distinct_product_in_status_claim_type,
        LAG(claimed_amount, 3, 0) OVER (ORDER BY premium, id) AS prev3_claimed_amount
    FROM in_window
)
SELECT
    id,
    policy_id,
    claim_date,
    premium,
    distinct_product_in_status_claim_type,
    prev3_claimed_amount,
    DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
    CASE WHEN premium >= 2945.9 THEN 'large' WHEN premium >= 2289.0 THEN 'medium' ELSE 'small' END AS premium_tier
FROM with_trends
WHERE distinct_product_in_status_claim_type > 3
ORDER BY claim_date NULLS LAST
