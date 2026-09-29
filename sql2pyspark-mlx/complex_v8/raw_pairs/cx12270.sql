WITH joined_ranges AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        premium,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        ROUND(AVG(premium) OVER w_claim_type, 2) AS claim_type_avg_premium,
        SIZE(COLLECT_SET(product) OVER w_claim_type) AS distinct_product_in_claim_type,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY claim_type), 2) AS claim_type_avg_claimed_amount
    FROM joined_ranges
    WINDOW w_claim_type AS (PARTITION BY claim_type)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.premium,
        s.second_approved_amount,
        s.distinct_product_in_claim_type,
        s.claim_type_avg_claimed_amount,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id
    FROM ranked s
    JOIN policies p ON p.id = s.policy_id AND p.status <> 'CANCELLED'
)
SELECT
    id,
    policy_id,
    premium,
    second_approved_amount,
    distinct_product_in_claim_type,
    claim_type_avg_claimed_amount,
    policy_holder_id,
    COALESCE(policy_premium, 0.0) AS policy_premium_filled
FROM with_details
ORDER BY premium NULLS LAST, distinct_product_in_claim_type
