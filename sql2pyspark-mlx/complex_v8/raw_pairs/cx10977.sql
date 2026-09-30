WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE YEAR(claim_date) = 2025
      AND adjuster_id >= 2
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM valid_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY adjuster_team ORDER BY reported_at DESC, id) AS reported_at_rank,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id), 0), 4) AS approved_amount_share_of_policy_id,
        ROUND(approved_amount - AVG(approved_amount) OVER w_policy_id, 2) AS approved_amount_vs_policy_id_avg,
        LEAD(approved_amount) OVER w_all_by_approved_amount AS next_approved_amount,
        SUM(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_sum_approved_amount,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY adjuster_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        LAST_VALUE(reported_at) OVER (PARTITION BY adjuster_team ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        approved_amount - LAG(approved_amount) OVER w_adjuster_team_by_reported_at AS approved_amount_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount DESC), 3) AS approved_amount_percent_rank,
        ROUND(PERCENT_RANK() OVER w_adjuster_id_by_reported_at_t, 3) AS reported_at_percent_rank,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        DENSE_RANK() OVER (ORDER BY approved_amount DESC, id) AS approved_amount_dense_rank
    FROM with_details
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id), w_adjuster_team_by_reported_at AS (PARTITION BY adjuster_team ORDER BY reported_at DESC, id), w_adjuster_id_by_reported_at_t AS (PARTITION BY adjuster_id ORDER BY reported_at DESC)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE approved_amount_dense_rank = 1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        status,
        policy_id,
        ROUND(SUM(next_approved_amount) / NULLIF(SUM(approved_amount), 0), 3) AS next_approved_amount_to_approved_amount_ratio,
        COUNT(*) AS row_count,
        MAX(approved_amount_share_of_policy_id) AS max_approved_amount_share_of_policy_id,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        SUM(CASE WHEN adjuster_team IS NOT NULL THEN 1 ELSE 0 END) AS n_adjuster_team_is_not_null
    FROM top_per_group
    GROUP BY adjuster_id, status, policy_id
)
-- Final projection
SELECT
    adjuster_id,
    status,
    next_approved_amount_to_approved_amount_ratio,
    max_approved_amount_share_of_policy_id,
    distinct_policy_ids
FROM rollup_base
ORDER BY max_approved_amount_share_of_policy_id DESC
