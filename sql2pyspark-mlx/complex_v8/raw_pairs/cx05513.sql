WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 8 AND 11
      AND approved_amount > 4868.7
),
combined_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.status AS policy_status,
        p.end_date AS policy_end_date
    FROM cleaned_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND a.team = 'Travel'
    LEFT JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY policy_status ORDER BY policy_end_date) AS policy_end_date_dense_rank,
        ROUND(PERCENT_RANK() OVER w_policy_id_claim_type_by_adju, 3) AS adjuster_level_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_status ORDER BY policy_end_date), 3) AS policy_end_date_percent_rank,
        MIN(adjuster_level) OVER (PARTITION BY policy_id, claim_type ORDER BY policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_adjuster_level_so_far,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY policy_status), 0), 4) AS adjuster_level_share_of_policy_status,
        adjuster_level - LAG(adjuster_level) OVER (PARTITION BY policy_status, policy_id ORDER BY policy_end_date DESC, id) AS adjuster_level_change,
        SUM(adjuster_level) OVER (PARTITION BY policy_status, policy_id ORDER BY policy_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjuster_level,
        MIN(adjuster_level) OVER (PARTITION BY policy_status) AS policy_status_min_adjuster_level,
        MAX(adjuster_level) OVER (PARTITION BY policy_id, claim_type ORDER BY adjuster_level DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS adjuster_level_max_6_prior,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY policy_id), 2) AS adjuster_level_vs_policy_id_avg
    FROM combined_details_adjuster
    WINDOW w_policy_id_claim_type_by_adju AS (PARTITION BY policy_id, claim_type ORDER BY adjuster_level DESC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(running_adjuster_level) IGNORE NULLS OVER (ORDER BY adjuster_level ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_adjuster_level,
        NTILE(4) OVER (PARTITION BY claim_type ORDER BY adjuster_level_percent_rank DESC, id) AS adjuster_level_percent_rank_quartile,
        ROUND(adjuster_level_percent_rank - AVG(adjuster_level_percent_rank) OVER (PARTITION BY policy_status), 2) AS adjuster_level_percent_rank_vs_policy_status_avg
    FROM metrics
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    status,
    adjuster_level,
    policy_end_date,
    policy_end_date_dense_rank,
    adjuster_level_percent_rank,
    adjuster_level_share_of_policy_status,
    adjuster_level_change,
    first_known_running_adjuster_level,
    adjuster_level_percent_rank_quartile,
    adjuster_level_percent_rank_vs_policy_status_avg,
    SUBSTRING(TRIM(policy_status), 1, 2) AS policy_status_prefix
FROM ranked
ORDER BY policy_status_prefix DESC NULLS FIRST, id
LIMIT 10
