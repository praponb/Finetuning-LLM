WITH prepared AS (
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
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND claim_date IS NOT NULL
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.*,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team,
        p.end_date AS policy_end_date
    FROM prepared s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        adjuster_level,
        adjuster_name,
        adjuster_team,
        policy_end_date,
        MIN(adjuster_level) OVER (PARTITION BY claim_type ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_adjuster_level_so_far,
        NTILE(10) OVER (PARTITION BY status, claim_date_dayofweek ORDER BY policy_end_date ASC, id) AS policy_end_date_decile,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY adjuster_team, status ORDER BY policy_end_date, id) AS claimed_amount_change,
        SUM(adjuster_level) OVER (PARTITION BY status, adjuster_team) AS status_adjuster_team_sum_adjuster_level,
        NTH_VALUE(policy_end_date, 2) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_end_date,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS approved_amount_vs_claim_type_avg,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status, adjuster_team), 2) AS approved_amount_vs_status_adjuster_team_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_team, status ORDER BY policy_end_date), 3) AS policy_end_date_percent_rank,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_adjuster_level_claim_date_mo, 2) AS claimed_amount_vs_adjuster_level_claim_date_mont,
        ROUND(AVG(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_3_prior,
        MAX(claimed_amount) OVER (PARTITION BY status, adjuster_team) AS status_adjuster_team_max_claimed_amount,
        ROUND(claim_date_dayofweek - AVG(claim_date_dayofweek) OVER (PARTITION BY adjuster_level, DATE_TRUNC('MONTH', claim_date)), 2) AS claim_date_dayofweek_vs_adjuster_level_claim_dat,
        NTH_VALUE(claim_date_dayofweek, 2) OVER (PARTITION BY adjuster_team, status ORDER BY policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date_dayofweek,
        SIZE(COLLECT_SET(adjuster_team) OVER (PARTITION BY adjuster_level, DATE_TRUNC('MONTH', claim_date))) AS distinct_adjuster_team_in_adjuster_level_claim_d,
        ROUND(adjuster_level - AVG(adjuster_level) OVER w_claim_type, 2) AS adjuster_level_vs_claim_type_avg
    FROM joined_adjuster
    WINDOW w_adjuster_level_claim_date_mo AS (PARTITION BY adjuster_level, DATE_TRUNC('MONTH', claim_date)), w_claim_type AS (PARTITION BY claim_type)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(claimed_amount_change, adjuster_level) AS greatest_claimed_amount_change_adjuster_level
    FROM metrics
    WHERE min_adjuster_level_so_far BETWEEN 6 AND 43
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(policy_end_date_percent_rank - AVG(policy_end_date_percent_rank) OVER w_distinct_adjuster_team_in_ad, 2) AS policy_end_date_percent_rank_vs_distinct_adjuste,
        SUM(adjuster_level) OVER w_distinct_adjuster_team_in_ad AS distinct_adjuster_team_in_adjuster_level_claim_d_2,
        adjuster_level - LAG(adjuster_level) OVER (PARTITION BY policy_end_date_decile ORDER BY second_policy_end_date DESC, id) AS adjuster_level_change,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY distinct_adjuster_team_in_adjuster_level_claim_d), 0), 4) AS adjuster_level_share_of_distinct_adjuster_team_i,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY distinct_adjuster_team_in_adjuster_level_claim_d)) AS distinct_claim_type_in_distinct_adjuster_team_in,
        MAX(approved_amount_avg_3_prior) OVER (PARTITION BY distinct_adjuster_team_in_adjuster_level_claim_d) AS distinct_adjuster_team_in_adjuster_level_claim_d_3
    FROM tagged
    WINDOW w_distinct_adjuster_team_in_ad AS (PARTITION BY distinct_adjuster_team_in_adjuster_level_claim_d)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_end_date_decile,
        adjuster_id,
        ROUND(STDDEV(approved_amount_vs_status_adjuster_team_avg), 2) AS stddev_approved_amount_vs_status_adjuster_team_a,
        ROUND(STDDEV(approved_amount), 2) AS stddev_approved_amount,
        ROUND(SUM(distinct_adjuster_team_in_adjuster_level_claim_d_2) / NULLIF(SUM(policy_end_date_decile), 0), 3) AS distinct_adjuster_team_in_adjuster_level_claim_d_4,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
    FROM sequenced
    WHERE distinct_adjuster_team_in_adjuster_level_claim_d_3 > 125000.0
    GROUP BY 1, 2
)
SELECT
    policy_end_date_decile,
    stddev_approved_amount,
    distinct_adjuster_ids,
    ROUND(distinct_adjuster_ids / NULLIF(stddev_approved_amount, 0), 3) AS distinct_adjuster_ids_per_stddev_approved_amount
FROM rollup_base
WHERE policy_end_date_decile >= 5
ORDER BY stddev_approved_amount, distinct_adjuster_ids_per_stddev_approved_amount DESC
