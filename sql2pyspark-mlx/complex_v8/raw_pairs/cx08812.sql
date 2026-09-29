WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE claimed_amount BETWEEN 4868.7 AND 44921.3
      AND claim_type IN ('delay', 'fire', 'theft')
),
joined AS (
    -- Join policies
    SELECT
        s.*,
        p.status AS policy_status,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM scoped_claims s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.status NOT IN ('CANCELLED', 'LAPSED')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(approved_amount) OVER (PARTITION BY policy_status, policy_product) AS policy_status_policy_product_max_approved_amount,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_status, policy_product)) AS distinct_adjuster_id_in_policy_status_policy_pro,
        ROUND(approved_amount - AVG(approved_amount) OVER w_policy_id, 2) AS approved_amount_vs_policy_id_avg,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_product ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        COUNT(*) OVER (PARTITION BY policy_product ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (PARTITION BY policy_product ORDER BY approved_amount DESC) AS approved_amount_dense_rank,
        LAG(policy_premium, 1, 0) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id) AS prev_policy_premium,
        RANK() OVER (PARTITION BY policy_product, adjuster_id ORDER BY claim_date ASC NULLS LAST, id) AS claim_date_rank,
        policy_premium - LAG(policy_premium) OVER w_policy_product_adjuster_id_b AS policy_premium_change
    FROM joined
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_policy_product_adjuster_id_b AS (PARTITION BY policy_product, adjuster_id ORDER BY claim_date ASC NULLS LAST, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        approved_amount_minus_claimed_amount,
        policy_status,
        policy_premium,
        policy_product,
        policy_status_policy_product_max_approved_amount,
        distinct_adjuster_id_in_policy_status_policy_pro,
        approved_amount_vs_policy_id_avg,
        second_claim_date,
        running_count,
        prev_policy_premium,
        claim_date_rank,
        policy_premium_change
    FROM ranked
    WHERE approved_amount_dense_rank = 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        status,
        approved_amount_minus_claimed_amount,
        policy_premium,
        policy_status_policy_product_max_approved_amount,
        approved_amount_vs_policy_id_avg,
        second_claim_date,
        prev_policy_premium,
        claim_date_rank,
        policy_premium_change,
        DENSE_RANK() OVER w_claim_date_rank_by_claim_dat AS claim_date_dense_rank,
        FIRST_VALUE(policy_premium) IGNORE NULLS OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_policy_premium,
        SUM(policy_premium_change) OVER (PARTITION BY policy_id) AS policy_id_sum_policy_premium_change,
        ROUND(claim_date_rank / NULLIF(SUM(claim_date_rank) OVER w_policy_id, 0), 4) AS claim_date_rank_share_of_policy_id,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_policy_id, 2) AS claimed_amount_vs_policy_id_avg,
        ROUND(AVG(prev_policy_premium) OVER (PARTITION BY claim_date_rank ORDER BY claim_date ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS prev_policy_premium_avg_3,
        SUM(approved_amount) OVER (ORDER BY claim_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS approved_amount_sum_7
    FROM top_per_group
    WINDOW w_claim_date_rank_by_claim_dat AS (PARTITION BY claim_date_rank ORDER BY claim_date ASC), w_policy_id AS (PARTITION BY policy_id)
)
-- Final projection
SELECT
    id,
    status,
    approved_amount_minus_claimed_amount,
    policy_premium,
    approved_amount_vs_policy_id_avg,
    second_claim_date,
    prev_policy_premium,
    claim_date_rank,
    policy_premium_change,
    policy_id_sum_policy_premium_change,
    claim_date_rank_share_of_policy_id,
    claimed_amount_vs_policy_id_avg,
    prev_policy_premium_avg_3,
    approved_amount_sum_7
FROM windowed
ORDER BY policy_id_sum_policy_premium_change
