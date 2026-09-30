WITH matched AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        product,
        SUM(premium) OVER (PARTITION BY product, claim_type) AS product_claim_type_sum_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product_claim_type, 0), 4) AS premium_share_of_product_claim_type,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY policy_id, product ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        SUM(claimed_amount) OVER w_product_claim_type AS product_claim_type_sum_claimed_amount
    FROM matched
    WINDOW w_product_claim_type AS (PARTITION BY product, claim_type)
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claimed_amount BETWEEN 10673.3 AND 38909.4
              AND c.reported_at < TIMESTAMP '2025-03-08 14:00:00'
        )
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.product,
        s.product_claim_type_sum_premium,
        s.product_claim_type_sum_claimed_amount,
        p.end_date AS policy_end_date,
        p.product AS policy_product
    FROM screened s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    product,
    product_claim_type_sum_premium,
    product_claim_type_sum_claimed_amount,
    policy_product,
    COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
    DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date
FROM with_details_policy
ORDER BY product_claim_type_sum_claimed_amount, id DESC
