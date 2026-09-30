WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE claim_type NOT IN ('theft', 'medical')
      AND adjuster_id IS NOT NULL
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_bucket,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM valid s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    WHERE p.premium < 4678.8
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(adjuster_level) OVER (PARTITION BY status ORDER BY claimed_amount_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_adjuster_level,
        LAG(adjuster_level) OVER (PARTITION BY status ORDER BY claimed_amount_bucket DESC, id) AS prev_adjuster_level,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount ASC, id), 3) AS approved_amount_cume_dist,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id, adjuster_level)) AS distinct_adjuster_id_in_policy_id_adjuster_level,
        ROUND(AVG(policy_premium) OVER (PARTITION BY policy_id, adjuster_level), 2) AS policy_id_adjuster_level_avg_policy_premium,
        ROUND(claimed_amount_bucket - AVG(claimed_amount_bucket) OVER w_status, 2) AS claimed_amount_bucket_vs_status_avg,
        MAX(approved_amount) OVER (PARTITION BY status ORDER BY approved_amount DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_max_6_prior,
        SUM(approved_amount) OVER (PARTITION BY policy_id ORDER BY policy_premium, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_6_prior,
        MAX(approved_amount) OVER (PARTITION BY status) AS status_max_approved_amount,
        SIZE(COLLECT_SET(policy_holder_id) OVER (PARTITION BY status)) AS distinct_policy_holder_id_in_status,
        ROUND(claimed_amount_bucket / NULLIF(SUM(claimed_amount_bucket) OVER w_status, 0), 4) AS claimed_amount_bucket_share_of_status,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY approved_amount DESC) AS approved_amount_dense_rank,
        COUNT(*) OVER (PARTITION BY policy_id, adjuster_level) AS rows_in_policy_id_adjuster_level
    FROM combined_details
    WINDOW w_status AS (PARTITION BY status), w_policy_id_adjuster_level AS (PARTITION BY policy_id, adjuster_level)
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        approved_amount,
        reported_at,
        policy_premium,
        policy_holder_id,
        adjuster_level,
        adjuster_name,
        first_adjuster_level,
        prev_adjuster_level,
        distinct_adjuster_id_in_policy_id_adjuster_level,
        policy_id_adjuster_level_avg_policy_premium,
        approved_amount_max_6_prior,
        approved_amount_sum_6_prior,
        status_max_approved_amount,
        distinct_policy_holder_id_in_status,
        claimed_amount_bucket_share_of_status,
        approved_amount_dense_rank,
        rows_in_policy_id_adjuster_level,
        FIRST_VALUE(first_adjuster_level) IGNORE NULLS OVER (ORDER BY rows_in_policy_id_adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_adjuster_level,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status)) AS distinct_policy_id_in_status,
        ROUND(approved_amount_cume_dist / NULLIF(SUM(approved_amount_cume_dist) OVER w_status, 0), 4) AS approved_amount_cume_dist_share_of_status,
        ROUND(CUME_DIST() OVER (ORDER BY rows_in_policy_id_adjuster_level DESC, id), 3) AS rows_in_policy_id_adjuster_level_cume_dist,
        RANK() OVER w_all_by_rows_in_policy_id_adj_3 AS rows_in_policy_id_adjuster_level_rank,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY status ORDER BY reported_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_7,
        ROUND(AVG(first_adjuster_level) OVER w_status, 2) AS status_avg_first_adjuster_level,
        MIN(approved_amount_cume_dist) OVER w_status AS status_min_approved_amount_cume_dist
    FROM metrics
    WINDOW w_status AS (PARTITION BY status), w_all_by_rows_in_policy_id_adj_3 AS (ORDER BY rows_in_policy_id_adjuster_level DESC)
),
per_group AS (
    -- Group-level totals
    SELECT
        policy_holder_id,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        COUNT(DISTINCT policy_holder_id) AS distinct_policy_holder_ids
    FROM metrics_2
    WHERE policy_holder_id <= 8
    GROUP BY 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        policy_holder_id,
        adjuster_name_list,
        distinct_policy_holder_ids,
        NTILE(4) OVER (PARTITION BY policy_holder_id, distinct_policy_holder_ids ORDER BY distinct_policy_holder_ids ASC, policy_holder_id) AS distinct_policy_holder_ids_quartile,
        ROUND(AVG(distinct_policy_holder_ids) OVER (PARTITION BY distinct_policy_holder_ids, policy_holder_id), 2) AS distinct_policy_holder_ids_policy_holder_id_avg_,
        ROUND(distinct_policy_holder_ids / NULLIF(SUM(distinct_policy_holder_ids) OVER (PARTITION BY distinct_policy_holder_ids, policy_holder_id), 0), 4) AS distinct_policy_holder_ids_share_of_distinct_pol,
        FIRST_VALUE(distinct_policy_holder_ids) IGNORE NULLS OVER (ORDER BY distinct_policy_holder_ids DESC, policy_holder_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_policy_holder_ids
    FROM per_group
)
SELECT
    policy_holder_id,
    adjuster_name_list,
    distinct_policy_holder_ids_quartile,
    distinct_policy_holder_ids_policy_holder_id_avg_,
    first_known_distinct_policy_holder_ids,
    SIZE(adjuster_name_list) AS adjuster_name_list_count
FROM windowed
ORDER BY adjuster_name_list_count DESC, distinct_policy_holder_ids_policy_holder_id_avg_ DESC
