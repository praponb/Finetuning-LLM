WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE claimed_amount BETWEEN 6745.1 AND 32441.2
      AND claim_date IS NOT NULL
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        p.product AS policy_product,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id,
        a.level AS adjuster_level
    FROM recent_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        policy_product,
        policy_end_date,
        policy_holder_id,
        adjuster_level,
        SIZE(COLLECT_SET(policy_holder_id) OVER w_policy_end_date_month) AS distinct_policy_holder_id_in_policy_end_date_mon,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY MONTH(policy_end_date)), 0), 4) AS claimed_amount_share_of_policy_end_date_month,
        DENSE_RANK() OVER (ORDER BY claimed_amount DESC, id) AS claimed_amount_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY claimed_amount DESC, id), 3) AS claimed_amount_percent_rank,
        MAX(approved_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY MONTH(policy_end_date)), 2) AS adjuster_level_vs_policy_end_date_month_avg,
        SIZE(COLLECT_SET(policy_id) OVER w_status_adjuster_id) AS distinct_policy_id_in_status_adjuster_id,
        LAST_VALUE(policy_end_date) OVER (ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_policy_end_date,
        ROUND(AVG(claimed_amount) OVER (ORDER BY approved_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_4,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY status, adjuster_id), 0), 4) AS approved_amount_share_of_status_adjuster_id
    FROM enriched
    WINDOW w_policy_end_date_month AS (PARTITION BY MONTH(policy_end_date)), w_status_adjuster_id AS (PARTITION BY status, adjuster_id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        policy_product,
        policy_end_date,
        policy_holder_id,
        adjuster_level,
        claimed_amount_dense_rank,
        claimed_amount_percent_rank,
        approved_amount_max_3,
        adjuster_level_vs_policy_end_date_month_avg,
        distinct_policy_id_in_status_adjuster_id,
        claimed_amount_avg_4,
        approved_amount_share_of_status_adjuster_id,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY policy_product, policy_id), 2) AS adjuster_level_vs_policy_product_policy_id_avg,
        ROUND(claimed_amount_avg_4 / NULLIF(SUM(claimed_amount_avg_4) OVER (PARTITION BY policy_product, policy_id), 0), 4) AS claimed_amount_avg_4_share_of_policy_product_pol,
        LAG(adjuster_level, 3) OVER (ORDER BY adjuster_level_vs_policy_end_date_month_avg ASC, id) AS prev3_adjuster_level,
        MIN(adjuster_level) OVER (ORDER BY policy_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_adjuster_level_so_far
    FROM metrics
)
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    policy_product,
    policy_holder_id,
    adjuster_level,
    claimed_amount_dense_rank,
    approved_amount_max_3,
    adjuster_level_vs_policy_end_date_month_avg,
    distinct_policy_id_in_status_adjuster_id,
    approved_amount_share_of_status_adjuster_id,
    adjuster_level_vs_policy_product_policy_id_avg,
    claimed_amount_avg_4_share_of_policy_product_pol,
    prev3_adjuster_level,
    LEAST(prev3_adjuster_level, claimed_amount_avg_4) AS least_prev3_adjuster_level_claimed_amount_avg_4,
    ROUND(approved_amount_max_3 / NULLIF(claimed_amount_avg_4, 0), 3) AS approved_amount_max_3_per_claimed_amount_avg_4
FROM with_trends
ORDER BY id, adjuster_level_vs_policy_end_date_month_avg
