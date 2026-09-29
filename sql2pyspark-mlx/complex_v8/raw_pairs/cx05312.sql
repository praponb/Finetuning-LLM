WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.status,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        claimed_amount,
        product,
        premium,
        QUARTER(claim_date) AS claim_date_quarter,
        ROUND(premium / NULLIF(claimed_amount, 0), 3) AS premium_per_claimed_amount,
        ADD_MONTHS(claim_date, 1) AS claim_date_plus_1m
    FROM joined_ranges
    WHERE claim_date >= DATE '2024-03-24'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY claim_date_quarter ORDER BY claim_date_plus_1m, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        LAG(premium) OVER w_policy_id_product_by_claim_d AS prev_premium,
        SUM(claimed_amount) OVER (PARTITION BY claim_date_quarter ORDER BY claim_date_plus_1m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        ROUND(premium_per_claimed_amount / NULLIF(SUM(premium_per_claimed_amount) OVER (PARTITION BY claim_date_quarter), 0), 4) AS premium_per_claimed_amount_share_of_claim_date_q,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id, product ORDER BY claim_date_plus_1m ASC, id), 3) AS claim_date_plus_1m_percent_rank
    FROM annotated
    WINDOW w_policy_id_product_by_claim_d AS (PARTITION BY policy_id, product ORDER BY claim_date_plus_1m ASC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        premium,
        claim_date_quarter,
        claim_date_plus_1m,
        prev_premium,
        premium_per_claimed_amount_share_of_claim_date_q,
        claim_date_plus_1m_percent_rank,
        SUM(claim_date_quarter) OVER (PARTITION BY product ORDER BY last_claimed_amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claim_date_quarter_sum_3_prior,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY product)) AS distinct_policy_id_in_product,
        ROUND(AVG(last_claimed_amount) OVER (ORDER BY premium DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS last_claimed_amount_avg_6_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_date_quarter ORDER BY claim_date_plus_1m_percent_rank), 3) AS claim_date_plus_1m_percent_rank_cume_dist,
        prev_premium - LAG(prev_premium) OVER (ORDER BY premium DESC, id) AS prev_premium_change,
        ROUND(prev_premium / NULLIF(SUM(prev_premium) OVER (PARTITION BY product), 0), 4) AS prev_premium_share_of_product
    FROM ranked
)
-- Final projection
SELECT
    id,
    policy_id,
    premium,
    claim_date_plus_1m,
    prev_premium,
    premium_per_claimed_amount_share_of_claim_date_q,
    claim_date_plus_1m_percent_rank,
    claim_date_quarter_sum_3_prior,
    distinct_policy_id_in_product,
    prev_premium_change,
    prev_premium_share_of_product,
    GREATEST(prev_premium_change, claim_date_quarter) AS greatest_prev_premium_change_claim_date_quarter
FROM metrics
WHERE premium_per_claimed_amount_share_of_claim_date_q BETWEEN 0.1 AND 0.7
ORDER BY distinct_policy_id_in_product DESC, greatest_prev_premium_change_claim_date_quarter NULLS LAST
