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
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(premium) OVER (PARTITION BY product, adjuster_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        SUM(approved_amount) OVER (PARTITION BY product, adjuster_id ORDER BY premium, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_1_prior,
        SUM(premium) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_sum_4,
        RANK() OVER (PARTITION BY product, adjuster_id ORDER BY premium, id) AS premium_rank
    FROM in_window
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), premium_rank ORDER BY claimed_amount ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_1_prior,
        RANK() OVER w_all_by_approved_amount AS approved_amount_rank,
        MAX(claimed_amount) OVER (PARTITION BY product) AS product_max_claimed_amount,
        ROUND(PERCENT_RANK() OVER w_claim_date_month_premium_ran_2, 3) AS claimed_amount_percent_rank,
        SUM(premium) OVER w_product AS product_sum_premium,
        FIRST_VALUE(premium) IGNORE NULLS OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium
    FROM metrics
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id), w_claim_date_month_premium_ran_2 AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), premium_rank ORDER BY claimed_amount ASC), w_product AS (PARTITION BY product)
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    approved_amount,
    adjuster_id,
    product,
    premium,
    premium_sum_4,
    approved_amount_rank,
    product_max_claimed_amount,
    claimed_amount_percent_rank,
    product_sum_premium,
    first_known_premium,
    DATE_ADD(claim_date, 7) AS claim_date_plus_7d,
    ROUND(claimed_amount / NULLIF(premium, 0), 3) AS claimed_amount_per_premium
FROM ranked
ORDER BY claimed_amount DESC, product
