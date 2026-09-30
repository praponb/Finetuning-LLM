WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM claims
    WHERE approved_amount <= 20880.4
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount_per_claimed_amount,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM base s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(adjuster_level, 2) OVER (PARTITION BY adjuster_level, policy_id ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY adjuster_level)) AS distinct_claim_type_in_adjuster_level,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY claim_type, adjuster_id), 2) AS claim_type_adjuster_id_avg_claimed_amount,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_level, policy_id ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS adjuster_level_avg_3_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY claimed_amount ASC), 3) AS claimed_amount_percent_rank,
        ROUND(AVG(claimed_amount) OVER w_adjuster_level, 2) AS adjuster_level_avg_claimed_amount,
        RANK() OVER (ORDER BY approved_amount_per_claimed_amount ASC, id) AS approved_amount_per_claimed_amount_rank,
        DENSE_RANK() OVER (PARTITION BY adjuster_level, policy_id ORDER BY claimed_amount DESC NULLS FIRST) AS claimed_amount_dense_rank,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY adjuster_level), 0), 4) AS adjuster_level_share_of_adjuster_level,
        MAX(claimed_amount) OVER (ORDER BY approved_amount_per_claimed_amount ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_1_prior,
        COUNT(*) OVER (PARTITION BY claim_type, adjuster_id) AS rows_in_claim_type_adjuster_id,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_id, adjuster_level ORDER BY adjuster_level, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_4,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY adjuster_level, policy_id ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        ROUND(PERCENT_RANK() OVER w_claim_type_by_claimed_amount, 3) AS claimed_amount_percent_rank_2,
        MIN(adjuster_level) OVER (PARTITION BY claim_type, adjuster_id) AS claim_type_adjuster_id_min_adjuster_level
    FROM enriched_adjuster
    WINDOW w_adjuster_level AS (PARTITION BY adjuster_level), w_claim_type_by_claimed_amount AS (PARTITION BY claim_type ORDER BY claimed_amount, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount_per_claimed_amount,
        adjuster_team,
        adjuster_level,
        adjuster_name,
        second_adjuster_level,
        distinct_claim_type_in_adjuster_level,
        claim_type_adjuster_id_avg_claimed_amount,
        adjuster_level_avg_3_prior,
        claimed_amount_percent_rank,
        adjuster_level_avg_claimed_amount,
        claimed_amount_dense_rank,
        adjuster_level_share_of_adjuster_level,
        claimed_amount_max_1_prior,
        rows_in_claim_type_adjuster_id,
        adjuster_level_avg_4,
        last_claimed_amount,
        claimed_amount_percent_rank_2,
        claim_type_adjuster_id_min_adjuster_level
    FROM metrics
    WHERE approved_amount_per_claimed_amount_rank = 1
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        adjuster_team,
        adjuster_name,
        distinct_claim_type_in_adjuster_level,
        claim_type_adjuster_id_avg_claimed_amount,
        adjuster_level_avg_claimed_amount,
        claimed_amount_dense_rank,
        adjuster_level_share_of_adjuster_level,
        rows_in_claim_type_adjuster_id,
        last_claimed_amount,
        MIN(last_claimed_amount) OVER (ORDER BY claimed_amount_dense_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_last_claimed_amount_so_far,
        adjuster_level - LAG(adjuster_level) OVER (PARTITION BY adjuster_team ORDER BY adjuster_level_avg_4, id) AS adjuster_level_change,
        ROUND(PERCENT_RANK() OVER w_distinct_claim_type_in_adjus, 3) AS adjuster_level_avg_4_percent_rank,
        SUM(adjuster_level) OVER w_distinct_claim_type_in_adjus_2 AS distinct_claim_type_in_adjuster_level_sum_adjust,
        NTH_VALUE(adjuster_level_avg_4, 2) OVER (PARTITION BY adjuster_team ORDER BY adjuster_level_avg_4, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level_avg_4,
        MIN(claimed_amount_dense_rank) OVER (PARTITION BY distinct_claim_type_in_adjuster_level) AS distinct_claim_type_in_adjuster_level_min_claime,
        SUM(adjuster_level_avg_3_prior) OVER w_adjuster_team AS adjuster_team_sum_adjuster_level_avg_3_prior
    FROM top_per_group
    WINDOW w_distinct_claim_type_in_adjus AS (PARTITION BY distinct_claim_type_in_adjuster_level ORDER BY adjuster_level_avg_4 ASC, id), w_distinct_claim_type_in_adjus_2 AS (PARTITION BY distinct_claim_type_in_adjuster_level), w_adjuster_team AS (PARTITION BY adjuster_team)
),
per_group AS (
    -- Group-level totals
    SELECT
        CASE WHEN last_claimed_amount >= 33690.0 THEN 'A' WHEN last_claimed_amount >= 13635.1 THEN 'B' ELSE 'C' END AS last_claimed_amount_tier,
        adjuster_level_change,
        COUNT(*) AS row_count,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        ROUND(SUM(adjuster_level_avg_4_percent_rank) / NULLIF(SUM(distinct_claim_type_in_adjuster_level_min_claime), 0), 3) AS adjuster_level_avg_4_percent_rank_to_distinct_cl,
        COUNT(DISTINCT adjuster_team) AS distinct_adjuster_teams
    FROM metrics_2
    GROUP BY CASE WHEN last_claimed_amount >= 33690.0 THEN 'A' WHEN last_claimed_amount >= 13635.1 THEN 'B' ELSE 'C' END, adjuster_level_change
    HAVING AVG(adjuster_level_share_of_adjuster_level) > 0.2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        last_claimed_amount_tier,
        adjuster_level_change,
        distinct_policy_ids,
        distinct_adjuster_ids,
        distinct_adjuster_teams,
        ROUND(distinct_policy_ids - AVG(distinct_policy_ids) OVER (PARTITION BY distinct_adjuster_teams, distinct_adjuster_ids), 2) AS distinct_policy_ids_vs_distinct_adjuster_teams_d,
        SIZE(COLLECT_SET(last_claimed_amount_tier) OVER (PARTITION BY distinct_adjuster_teams, distinct_adjuster_ids)) AS distinct_last_claimed_amount_tier_in_distinct_ad
    FROM per_group
)
-- Final projection
SELECT
    last_claimed_amount_tier,
    adjuster_level_change,
    distinct_policy_ids,
    distinct_adjuster_ids,
    distinct_adjuster_teams,
    distinct_last_claimed_amount_tier_in_distinct_ad,
    CASE WHEN distinct_policy_ids >= 9 THEN 'A' WHEN distinct_policy_ids >= 4 THEN 'B' ELSE 'C' END AS distinct_policy_ids_tier,
    ABS(adjuster_level_change - 0) AS adjuster_level_change_distance
FROM ranked
WHERE distinct_policy_ids < 9
ORDER BY adjuster_level_change DESC
