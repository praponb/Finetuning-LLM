WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CASE WHEN claim_type IN ('flood', 'theft') THEN 'group_a' WHEN claim_type = 'fire' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        ROUND(approved_amount * 1.5, 2) AS approved_amount_adj,
        ABS(approved_amount - 9389.3) AS approved_amount_distance
    FROM claims
    WHERE adjuster_id >= 7
),
stats AS (
    -- Summarise per group
    SELECT
        policy_id,
        claim_type_group,
        COUNT(*) AS row_count,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        COUNT(DISTINCT claim_type_group) AS distinct_claim_type_groups,
        SUM(CASE WHEN status IN ('IN_REVIEW', 'REJECTED') THEN 1 ELSE 0 END) AS n_status_in_in_review_rejected
    FROM cleaned
    GROUP BY 1, 2
),
with_activity AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.product = 'HEALTH'
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY row_count)) AS distinct_policy_id_in_row_count,
        DENSE_RANK() OVER (ORDER BY row_count NULLS LAST, policy_id, claim_type_group) AS row_count_dense_rank
    FROM with_activity
)
-- Subtotals with GROUPING SETS
SELECT
    row_count_dense_rank,
    policy_id,
    n_status_in_in_review_rejected,
    GROUPING_ID() AS grouping_level,
    SUM(row_count_dense_rank) AS total_row_count_dense_rank,
    PERCENTILE_APPROX(distinct_adjuster_ids, 0.5) AS median_distinct_adjuster_ids,
    MAX(distinct_claim_type_groups) AS max_distinct_claim_type_groups
FROM with_trends
GROUP BY GROUPING SETS ((row_count_dense_rank, policy_id, n_status_in_in_review_rejected), (row_count_dense_rank))
ORDER BY row_count_dense_rank NULLS LAST, policy_id NULLS LAST, n_status_in_in_review_rejected NULLS LAST
