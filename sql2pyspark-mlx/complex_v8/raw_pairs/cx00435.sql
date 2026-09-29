WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at
    FROM claims
    WHERE adjuster_id >= 5
      AND MONTH(reported_at) IN (1, 2)
),
with_parent AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM prepared_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        LEAD(claim_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY adjuster_level DESC, id) AS next2_claim_date,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_adjuster_level,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY adjuster_level DESC, id) AS adjuster_level_dense_rank,
        MAX(adjuster_level) OVER (PARTITION BY adjuster_id ORDER BY adjuster_level DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS adjuster_level_max_3,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY adjuster_level DESC), 3) AS adjuster_level_cume_dist,
        FIRST_VALUE(adjuster_level) OVER (PARTITION BY claim_type ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_adjuster_level,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY policy_id), 0), 4) AS adjuster_level_share_of_policy_id,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY adjuster_level)) AS distinct_status_in_adjuster_level,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY adjuster_level DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_4,
        MAX(adjuster_level) OVER (PARTITION BY adjuster_level) AS adjuster_level_max_adjuster_level,
        NTH_VALUE(adjuster_level, 2) OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY adjuster_level DESC) AS adjuster_level_dense_rank_2
    FROM with_parent
)
SELECT
    id,
    claim_date,
    claim_type,
    status,
    adjuster_team,
    adjuster_level,
    last_known_claim_date,
    next2_claim_date,
    adjuster_level_dense_rank,
    adjuster_level_max_3,
    adjuster_level_cume_dist,
    first_adjuster_level,
    adjuster_level_share_of_policy_id,
    second_adjuster_level,
    adjuster_level_dense_rank_2
FROM with_trends
ORDER BY adjuster_level_share_of_policy_id DESC, adjuster_level_dense_rank_2 DESC, id
LIMIT 20
