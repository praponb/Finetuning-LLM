WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE status = 'IN_REVIEW'
),
with_parent AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.reported_at,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM base_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_level ORDER BY claimed_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_3,
        NTH_VALUE(adjuster_level, 2) OVER (PARTITION BY adjuster_level ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        LAG(claimed_amount, 3) OVER (ORDER BY adjuster_level NULLS LAST, id) AS prev3_claimed_amount,
        RANK() OVER w_all_by_adjuster_level_ties AS adjuster_level_rank,
        LEAD(adjuster_level, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY reported_at, id) AS next3_adjuster_level,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id), 0), 4) AS claimed_amount_share_of_adjuster_id,
        LAG(reported_at) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY reported_at, id) AS prev_reported_at,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_policy_id, 2) AS claimed_amount_vs_policy_id_avg,
        SUM(claimed_amount) OVER (ORDER BY adjuster_level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        MIN(adjuster_level) OVER (PARTITION BY policy_id) AS policy_id_min_adjuster_level,
        ROUND(CUME_DIST() OVER (ORDER BY adjuster_level NULLS LAST, id), 3) AS adjuster_level_cume_dist
    FROM with_parent
    WINDOW w_all_by_adjuster_level_ties AS (ORDER BY adjuster_level DESC), w_policy_id AS (PARTITION BY policy_id)
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(adjuster_level_avg_3 / NULLIF(SUM(adjuster_level_avg_3) OVER w_second_adjuster_level, 0), 4) AS adjuster_level_avg_3_share_of_second_adjuster_le,
        NTH_VALUE(adjuster_level_cume_dist, 2) OVER (PARTITION BY adjuster_level_rank, adjuster_team ORDER BY prev_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level_cume_dist,
        LEAD(reported_at, 3) OVER (PARTITION BY second_adjuster_level ORDER BY prev_reported_at, id) AS next3_reported_at,
        ROUND(AVG(next3_adjuster_level) OVER (PARTITION BY second_adjuster_level), 2) AS second_adjuster_level_avg_next3_adjuster_level,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY second_adjuster_level ORDER BY prev_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_level_rank, adjuster_team ORDER BY prev_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_level_rank, adjuster_team ORDER BY prev_reported_at, id), 3) AS prev_reported_at_cume_dist
    FROM ranked
    WINDOW w_second_adjuster_level AS (PARTITION BY second_adjuster_level)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    reported_at,
    adjuster_level,
    adjuster_team,
    adjuster_level_avg_3,
    second_adjuster_level,
    adjuster_level_rank,
    next3_adjuster_level,
    claimed_amount_share_of_adjuster_id,
    running_claimed_amount,
    policy_id_min_adjuster_level,
    adjuster_level_cume_dist,
    adjuster_level_avg_3_share_of_second_adjuster_le,
    second_adjuster_level_cume_dist,
    next3_reported_at,
    last_known_claimed_amount,
    prev_reported_at_cume_dist,
    LENGTH(TRIM(adjuster_team)) AS adjuster_team_len,
    QUARTER(prev_reported_at) AS prev_reported_at_quarter
FROM ranked_2
ORDER BY second_adjuster_level_cume_dist DESC, id
LIMIT 10
