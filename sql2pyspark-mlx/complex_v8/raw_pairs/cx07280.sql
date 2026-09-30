WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        ROUND(approved_amount * 0.9, 2) AS approved_amount_adj
    FROM claims
    WHERE claim_type = 'flood'
      AND approved_amount >= 0.0
),
enriched_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount_adj,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM valid s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount_adj,
        adjuster_name,
        adjuster_level,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_team), 2) AS adjuster_team_avg_adjuster_level,
        ROW_NUMBER() OVER (PARTITION BY adjuster_level ORDER BY approved_amount_adj, id) AS approved_amount_adj_seq,
        ROW_NUMBER() OVER (PARTITION BY claim_type, MONTH(claim_date) ORDER BY approved_amount_adj DESC, id) AS rn,
        SIZE(COLLECT_SET(adjuster_id) OVER w_adjuster_team) AS distinct_adjuster_id_in_adjuster_team,
        LAG(claim_date) OVER (PARTITION BY claim_type, MONTH(claim_date) ORDER BY approved_amount_adj DESC, id) AS prev_claim_date,
        MIN(adjuster_level) OVER (PARTITION BY adjuster_team) AS adjuster_team_min_adjuster_level,
        SUM(approved_amount_adj) OVER w_adjuster_team AS adjuster_team_sum_approved_amount_adj,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id ORDER BY claim_date), 3) AS claim_date_cume_dist,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY adjuster_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY adjuster_team), 2) AS adjuster_level_vs_adjuster_team_avg,
        SUM(approved_amount_adj) OVER (PARTITION BY adjuster_id ORDER BY claim_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS approved_amount_adj_sum_4,
        ROUND(AVG(approved_amount_adj) OVER (PARTITION BY adjuster_team), 2) AS adjuster_team_avg_approved_amount_adj,
        MAX(approved_amount_adj) OVER (PARTITION BY adjuster_team) AS adjuster_team_max_approved_amount_adj
    FROM enriched_adjuster
    WINDOW w_adjuster_team AS (PARTITION BY adjuster_team)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        adjuster_level,
        adjuster_team_avg_adjuster_level,
        approved_amount_adj_seq,
        rn,
        prev_claim_date,
        adjuster_team_min_adjuster_level,
        adjuster_team_sum_approved_amount_adj,
        claim_date_cume_dist,
        second_claim_date,
        adjuster_level_vs_adjuster_team_avg,
        approved_amount_adj_sum_4,
        adjuster_team_avg_approved_amount_adj,
        adjuster_team_max_approved_amount_adj,
        ROUND(approved_amount_adj_sum_4 / NULLIF(SUM(approved_amount_adj_sum_4) OVER (PARTITION BY adjuster_level, MONTH(second_claim_date)), 0), 4) AS approved_amount_adj_sum_4_share_of_adjuster_leve,
        LAST_VALUE(approved_amount_adj) OVER (PARTITION BY distinct_adjuster_id_in_adjuster_team, policy_id ORDER BY adjuster_team_avg_adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount_adj,
        LAST_VALUE(approved_amount_adj_sum_4) OVER (PARTITION BY MONTH(second_claim_date), policy_id ORDER BY claim_date_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount_adj_sum_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(second_claim_date), policy_id ORDER BY claim_date_cume_dist DESC), 3) AS claim_date_cume_dist_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_adjuster_id_in_adjuster_team, policy_id ORDER BY adjuster_team_avg_adjuster_level DESC, id), 3) AS adjuster_team_avg_adjuster_level_cume_dist,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY adjuster_level, MONTH(second_claim_date)), 2) AS adjuster_level_vs_adjuster_level_second_claim_da,
        ROW_NUMBER() OVER (PARTITION BY distinct_adjuster_id_in_adjuster_team, policy_id ORDER BY adjuster_team_avg_adjuster_level DESC, id) AS adjuster_team_avg_adjuster_level_seq,
        SUM(approved_amount_adj_seq) OVER (PARTITION BY adjuster_level, MONTH(second_claim_date)) AS adjuster_level_second_claim_date_month_sum_appro
    FROM with_windows
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_level,
    adjuster_team_avg_adjuster_level,
    approved_amount_adj_seq,
    rn,
    prev_claim_date,
    adjuster_team_min_adjuster_level,
    second_claim_date,
    adjuster_level_vs_adjuster_team_avg,
    approved_amount_adj_sum_4,
    adjuster_team_avg_approved_amount_adj,
    adjuster_team_max_approved_amount_adj,
    last_approved_amount_adj_sum_4,
    claim_date_cume_dist_percent_rank,
    adjuster_team_avg_adjuster_level_cume_dist,
    adjuster_level_vs_adjuster_level_second_claim_da,
    adjuster_team_avg_adjuster_level_seq,
    adjuster_level_second_claim_date_month_sum_appro
FROM with_trends
ORDER BY id
