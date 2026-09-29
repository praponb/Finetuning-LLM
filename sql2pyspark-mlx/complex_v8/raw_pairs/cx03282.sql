WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE policy_id <= 5
      AND claim_type NOT IN ('collision', 'flood')
),
with_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.reported_at,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM scoped s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        adjuster_name,
        adjuster_level,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC NULLS FIRST, id) AS claimed_amount_dense_rank,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY policy_id), 2) AS adjuster_level_vs_policy_id_avg,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id, adjuster_level ORDER BY adjuster_level NULLS LAST, id) AS claimed_amount_change,
        RANK() OVER (PARTITION BY adjuster_id ORDER BY adjuster_level) AS adjuster_level_rank,
        RANK() OVER (PARTITION BY policy_id, adjuster_level ORDER BY adjuster_level NULLS LAST) AS adjuster_level_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id ORDER BY adjuster_level), 3) AS adjuster_level_cume_dist,
        ROUND(CUME_DIST() OVER w_policy_id_by_adjuster_level, 3) AS adjuster_level_cume_dist_2,
        ROUND(CUME_DIST() OVER w_adjuster_id_by_claimed_amoun, 3) AS claimed_amount_cume_dist,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_3,
        RANK() OVER (ORDER BY claimed_amount) AS claimed_amount_rank,
        ROUND(PERCENT_RANK() OVER w_adjuster_id_by_adjuster_leve, 3) AS adjuster_level_percent_rank,
        LAG(claimed_amount) OVER w_all_by_claimed_amount AS prev_claimed_amount
    FROM with_details_adjuster
    WINDOW w_policy_id_by_adjuster_level AS (PARTITION BY policy_id ORDER BY adjuster_level, id), w_adjuster_id_by_claimed_amoun AS (PARTITION BY adjuster_id ORDER BY claimed_amount DESC NULLS FIRST, id), w_adjuster_id_by_adjuster_leve AS (PARTITION BY adjuster_id ORDER BY adjuster_level), w_all_by_claimed_amount AS (ORDER BY claimed_amount, id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    adjuster_name,
    adjuster_level,
    claimed_amount_dense_rank,
    adjuster_level_rank,
    adjuster_level_cume_dist_2,
    claimed_amount_cume_dist,
    claimed_amount_rank,
    adjuster_level_percent_rank,
    prev_claimed_amount
FROM with_windows
ORDER BY claimed_amount_rank DESC NULLS FIRST
