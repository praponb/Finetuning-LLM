WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix
    FROM claims
    WHERE claimed_amount >= 7204.1
      AND status = 'APPROVED'
),
combined_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        status,
        reported_at,
        claim_type_prefix,
        adjuster_name,
        adjuster_team,
        adjuster_level,
        policy_start_date,
        policy_end_date,
        MAX(adjuster_level) OVER (PARTITION BY status, adjuster_level) AS status_adjuster_level_max_adjuster_level,
        ROUND(AVG(adjuster_level) OVER (ORDER BY adjuster_level, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS adjuster_level_avg_1_prior
    FROM combined_details_adjuster
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY adjuster_level_avg_1_prior), 3) AS adjuster_level_avg_1_prior_cume_dist,
        ROUND(CUME_DIST() OVER w_all_by_adjuster_level_avg_1_2, 3) AS adjuster_level_avg_1_prior_cume_dist_2,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY adjuster_team, status), 2) AS adjuster_level_vs_adjuster_team_status_avg,
        RANK() OVER (ORDER BY policy_start_date DESC, id) AS policy_start_date_rank,
        SUM(adjuster_level_avg_1_prior) OVER (ORDER BY adjuster_level_avg_1_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS adjuster_level_avg_1_prior_sum_3,
        ROW_NUMBER() OVER (ORDER BY policy_start_date DESC, id) AS policy_start_date_seq
    FROM with_windows
    WINDOW w_all_by_adjuster_level_avg_1_2 AS (ORDER BY adjuster_level_avg_1_prior, id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(adjuster_level_avg_1_prior) OVER (PARTITION BY policy_id, policy_start_date_rank) AS policy_id_policy_start_date_rank_sum_adjuster_le,
        COUNT(*) OVER (PARTITION BY policy_id, policy_start_date_rank) AS rows_in_policy_id_policy_start_date_rank
    FROM with_windows_2
)
-- Final projection
SELECT
    id,
    policy_id,
    status,
    adjuster_team,
    adjuster_level,
    policy_start_date,
    adjuster_level_avg_1_prior_cume_dist_2,
    adjuster_level_vs_adjuster_team_status_avg,
    policy_start_date_rank,
    adjuster_level_avg_1_prior_sum_3,
    policy_id_policy_start_date_rank_sum_adjuster_le,
    ROUND(policy_start_date_rank / NULLIF(adjuster_level, 0), 3) AS policy_start_date_rank_per_adjuster_level
FROM windowed
WHERE policy_start_date_seq <= 7
