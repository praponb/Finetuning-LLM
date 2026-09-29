WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE policy_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id, claim_type)) AS distinct_adjuster_id_in_policy_id_claim_type,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, claim_type)) AS distinct_policy_id_in_adjuster_id_claim_type,
        MIN(approved_amount) OVER (PARTITION BY claim_type ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        NTILE(4) OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount ASC, id) AS approved_amount_quartile,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id)) AS distinct_claim_type_in_policy_id,
        LEAD(claimed_amount, 1, 0) OVER w_all_by_claimed_amount AS next_claimed_amount,
        MAX(approved_amount) OVER (PARTITION BY policy_id, claim_type) AS policy_id_claim_type_max_approved_amount,
        RANK() OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount ASC) AS approved_amount_rank,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        RANK() OVER (ORDER BY claimed_amount, id) AS claimed_amount_rank,
        ROUND(CUME_DIST() OVER w_policy_id_claim_type_by_appr, 3) AS approved_amount_cume_dist,
        LAG(approved_amount) OVER w_claim_type_by_claimed_amount AS prev_approved_amount,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount NULLS LAST, id), 3) AS approved_amount_percent_rank,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_3
    FROM cleaned
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount, id), w_policy_id_claim_type_by_appr AS (PARTITION BY policy_id, claim_type ORDER BY approved_amount ASC, id), w_claim_type_by_claimed_amount AS (PARTITION BY claim_type ORDER BY claimed_amount DESC NULLS FIRST, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        claim_type_slug,
        distinct_adjuster_id_in_policy_id_claim_type,
        distinct_policy_id_in_adjuster_id_claim_type,
        min_approved_amount_so_far,
        next_claimed_amount,
        policy_id_claim_type_max_approved_amount,
        claimed_amount_rank,
        prev_approved_amount,
        approved_amount_avg_3,
        ROUND(approved_amount_cume_dist / NULLIF(SUM(approved_amount_cume_dist) OVER (PARTITION BY distinct_claim_type_in_policy_id, adjuster_id), 0), 4) AS approved_amount_cume_dist_share_of_distinct_clai,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY distinct_claim_type_in_policy_id, adjuster_id), 2) AS approved_amount_vs_distinct_claim_type_in_policy,
        ROUND(PERCENT_RANK() OVER (PARTITION BY approved_amount_rank ORDER BY distinct_adjuster_id_in_policy_id_claim_type), 3) AS distinct_adjuster_id_in_policy_id_claim_type_per,
        ROUND(min_approved_amount_so_far / NULLIF(SUM(min_approved_amount_so_far) OVER w_distinct_claim_type_in_polic, 0), 4) AS min_approved_amount_so_far_share_of_distinct_cla,
        COUNT(*) OVER (PARTITION BY distinct_claim_type_in_policy_id, adjuster_id) AS rows_in_distinct_claim_type_in_policy_id_adjuste,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY distinct_adjuster_id_in_policy_id_claim_type, distinct_policy_id_in_adjuster_id_claim_type ORDER BY next_claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount
    FROM metrics
    WINDOW w_distinct_claim_type_in_polic AS (PARTITION BY distinct_claim_type_in_policy_id, adjuster_id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claimed_amount,
    approved_amount,
    claim_type_slug,
    distinct_policy_id_in_adjuster_id_claim_type,
    min_approved_amount_so_far,
    next_claimed_amount,
    policy_id_claim_type_max_approved_amount,
    claimed_amount_rank,
    prev_approved_amount,
    approved_amount_avg_3,
    approved_amount_cume_dist_share_of_distinct_clai,
    approved_amount_vs_distinct_claim_type_in_policy,
    distinct_adjuster_id_in_policy_id_claim_type_per,
    min_approved_amount_so_far_share_of_distinct_cla,
    rows_in_distinct_claim_type_in_policy_id_adjuste
FROM with_trends
ORDER BY policy_id_claim_type_max_approved_amount
