WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
),
joined AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.status AS policy_status
    FROM base s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
    WHERE a.level > 2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_all_by_reported_at_ties AS reported_at_rank,
        ROW_NUMBER() OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount DESC, id) AS approved_amount_seq,
        LAST_VALUE(approved_amount) OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY adjuster_level), 0), 4) AS approved_amount_share_of_adjuster_level,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY YEAR(reported_at), policy_status)) AS distinct_adjuster_id_in_reported_at_year_policy_,
        SUM(approved_amount) OVER (PARTITION BY status, adjuster_level ORDER BY approved_amount DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_1_prior,
        NTILE(10) OVER w_adjuster_level_adjuster_id_b AS approved_amount_decile,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY adjuster_level), 2) AS adjuster_level_vs_adjuster_level_avg,
        ROUND(AVG(approved_amount) OVER w_reported_at_year_policy_stat, 2) AS reported_at_year_policy_status_avg_approved_amou,
        SUM(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        MAX(approved_amount) OVER (PARTITION BY YEAR(reported_at), policy_status) AS reported_at_year_policy_status_max_approved_amou,
        MIN(adjuster_level) OVER w_reported_at_year_policy_stat AS reported_at_year_policy_status_min_adjuster_leve,
        RANK() OVER (PARTITION BY policy_id ORDER BY reported_at DESC) AS reported_at_rank_2,
        ROUND(AVG(approved_amount) OVER w_adjuster_level, 2) AS adjuster_level_avg_approved_amount,
        COUNT(*) OVER w_adjuster_level AS rows_in_adjuster_level,
        LAG(reported_at) OVER (ORDER BY reported_at, id) AS prev_reported_at
    FROM joined
    WINDOW w_all_by_reported_at_ties AS (ORDER BY reported_at), w_adjuster_level_adjuster_id_b AS (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount DESC, id), w_reported_at_year_policy_stat AS (PARTITION BY YEAR(reported_at), policy_status), w_adjuster_level AS (PARTITION BY adjuster_level)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_id,
        approved_amount,
        reported_at,
        adjuster_name,
        adjuster_level,
        reported_at_rank,
        approved_amount_share_of_adjuster_level,
        distinct_adjuster_id_in_reported_at_year_policy_,
        approved_amount_sum_1_prior,
        approved_amount_decile,
        adjuster_level_vs_adjuster_level_avg,
        running_approved_amount,
        reported_at_year_policy_status_min_adjuster_leve,
        reported_at_rank_2,
        adjuster_level_avg_approved_amount,
        prev_reported_at,
        ROUND(last_approved_amount * 1.5, 2) AS last_approved_amount_adj,
        LEAST(reported_at_rank_2, adjuster_level) AS least_reported_at_rank_2_adjuster_level,
        ROUND(approved_amount_seq / NULLIF(running_approved_amount, 0), 3) AS approved_amount_seq_per_running_approved_amount
    FROM sequenced
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        approved_amount,
        reported_at,
        reported_at_rank,
        adjuster_level_vs_adjuster_level_avg,
        running_approved_amount,
        reported_at_year_policy_status_min_adjuster_leve,
        reported_at_rank_2,
        adjuster_level_avg_approved_amount,
        prev_reported_at,
        last_approved_amount_adj,
        approved_amount_seq_per_running_approved_amount,
        SUM(reported_at_rank) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_reported_at_rank,
        LEAD(running_approved_amount, 1, 0) OVER (ORDER BY running_approved_amount DESC NULLS FIRST, id) AS next_running_approved_amount,
        ROUND(approved_amount_sum_1_prior - AVG(approved_amount_sum_1_prior) OVER w_distinct_adjuster_id_in_repo, 2) AS approved_amount_sum_1_prior_vs_distinct_adjuster,
        ROUND(reported_at_rank_2 - AVG(reported_at_rank_2) OVER (PARTITION BY distinct_adjuster_id_in_reported_at_year_policy_, least_reported_at_rank_2_adjuster_level), 2) AS reported_at_rank_2_vs_distinct_adjuster_id_in_re,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_id, distinct_adjuster_id_in_reported_at_year_policy_ ORDER BY distinct_adjuster_id_in_reported_at_year_policy_, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id, distinct_adjuster_id_in_reported_at_year_policy_ ORDER BY distinct_adjuster_id_in_reported_at_year_policy_, id) AS distinct_adjuster_id_in_reported_at_year_policy__2,
        ROUND(approved_amount_sum_1_prior / NULLIF(SUM(approved_amount_sum_1_prior) OVER w_adjuster_id, 0), 4) AS approved_amount_sum_1_prior_share_of_adjuster_id
    FROM annotated
    WINDOW w_distinct_adjuster_id_in_repo AS (PARTITION BY distinct_adjuster_id_in_reported_at_year_policy_, least_reported_at_rank_2_adjuster_level), w_adjuster_id AS (PARTITION BY adjuster_id)
)
SELECT
    id,
    adjuster_id,
    approved_amount,
    reported_at,
    reported_at_rank,
    adjuster_level_vs_adjuster_level_avg,
    running_approved_amount,
    reported_at_year_policy_status_min_adjuster_leve,
    reported_at_rank_2,
    adjuster_level_avg_approved_amount,
    prev_reported_at,
    last_approved_amount_adj,
    approved_amount_seq_per_running_approved_amount,
    adjuster_id_sum_reported_at_rank,
    approved_amount_sum_1_prior_vs_distinct_adjuster,
    reported_at_rank_2_vs_distinct_adjuster_id_in_re,
    distinct_adjuster_id_in_reported_at_year_policy__2,
    approved_amount_sum_1_prior_share_of_adjuster_id,
    ABS(approved_amount - 2349.3) AS approved_amount_distance,
    LEAST(last_approved_amount_adj, approved_amount) AS least_last_approved_amount_adj_approved_amount
FROM metrics
ORDER BY reported_at_rank_2 DESC
