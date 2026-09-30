WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE policy_id <= 13
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.product AS policy_product,
        p.end_date AS policy_end_date
    FROM cleaned s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        adjuster_name,
        policy_end_date,
        ROUND(AVG(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_1_prior,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY reported_at, id) AS reported_at_dense_rank,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY policy_id, policy_product), 2) AS adjuster_level_vs_policy_id_policy_product_avg,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY policy_id, adjuster_level), 2) AS policy_id_adjuster_level_avg_adjuster_level,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER w_policy_id_adjuster_level, 0), 4) AS adjuster_level_share_of_policy_id_adjuster_level,
        DENSE_RANK() OVER w_policy_id_by_policy_end_date AS policy_end_date_dense_rank,
        FIRST_VALUE(policy_end_date) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_policy_end_date,
        FIRST_VALUE(adjuster_level) IGNORE NULLS OVER (ORDER BY adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_adjuster_level,
        DATEDIFF(policy_end_date, LAG(policy_end_date) OVER (ORDER BY adjuster_level, id)) AS days_since_prev_policy_end_date,
        ROUND(PERCENT_RANK() OVER w_policy_id_by_policy_end_date_2, 3) AS policy_end_date_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id ORDER BY policy_end_date), 3) AS policy_end_date_cume_dist,
        SUM(approved_amount) OVER w_policy_id_adjuster_level AS policy_id_adjuster_level_sum_approved_amount
    FROM with_parent
    WINDOW w_policy_id_adjuster_level AS (PARTITION BY policy_id, adjuster_level), w_policy_id_by_policy_end_date AS (PARTITION BY policy_id ORDER BY policy_end_date, id), w_policy_id_by_policy_end_date_2 AS (PARTITION BY policy_id ORDER BY policy_end_date)
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(first_known_adjuster_level) OVER (ORDER BY approved_amount_avg_1_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_known_adjuster_level,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY policy_end_date_dense_rank ORDER BY adjuster_level_vs_policy_id_policy_product_avg ASC, id) AS approved_amount_change,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY first_known_adjuster_level, policy_end_date_dense_rank)) AS distinct_policy_id_in_first_known_adjuster_level,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY first_known_adjuster_level, policy_end_date_dense_rank)) AS distinct_adjuster_id_in_first_known_adjuster_lev,
        COUNT(*) OVER (PARTITION BY first_known_adjuster_level, policy_end_date_dense_rank) AS rows_in_first_known_adjuster_level_policy_end_da,
        LAG(policy_end_date, 3) OVER (PARTITION BY policy_end_date_dense_rank ORDER BY adjuster_level_vs_policy_id_policy_product_avg ASC, id) AS prev3_policy_end_date,
        ROW_NUMBER() OVER w_policy_end_date_dense_rank_b AS rn
    FROM windowed
    WINDOW w_policy_end_date_dense_rank_b AS (PARTITION BY policy_end_date_dense_rank ORDER BY adjuster_level_vs_policy_id_policy_product_avg ASC, id)
)
SELECT
    id,
    policy_id,
    approved_amount,
    status,
    reported_at,
    adjuster_name,
    policy_end_date,
    approved_amount_avg_1_prior,
    reported_at_dense_rank,
    adjuster_level_vs_policy_id_policy_product_avg,
    policy_id_adjuster_level_avg_adjuster_level,
    first_known_policy_end_date,
    first_known_adjuster_level,
    days_since_prev_policy_end_date,
    policy_end_date_percent_rank,
    policy_end_date_cume_dist,
    first_first_known_adjuster_level,
    distinct_adjuster_id_in_first_known_adjuster_lev,
    prev3_policy_end_date,
    rn,
    LEAST(approved_amount, rn) AS least_approved_amount_rn
FROM windowed_2
ORDER BY policy_id DESC NULLS FIRST
