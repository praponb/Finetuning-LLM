WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATE_ADD(claim_date, 7) AS claim_date_plus_7d
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
enriched_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        s.reported_at,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.status AS policy_status,
        p.product AS policy_product
    FROM scoped_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND a.adjuster_name IS NOT NULL
    JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(adjuster_level - AVG(adjuster_level) OVER w_claim_type, 2) AS adjuster_level_vs_claim_type_avg,
        LEAD(adjuster_level, 2) OVER (ORDER BY claim_date, id) AS next2_adjuster_level,
        ROUND(adjuster_level - AVG(adjuster_level) OVER w_status_reported_at_to_date, 2) AS adjuster_level_vs_status_reported_at_to_date_avg,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        RANK() OVER w_status_adjuster_id_by_adjust AS adjuster_level_rank,
        SIZE(COLLECT_SET(policy_product) OVER (PARTITION BY status, TO_DATE(reported_at))) AS distinct_policy_product_in_status_reported_at_to,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY claim_type, policy_id), 2) AS claim_type_policy_id_avg_adjuster_level,
        MIN(adjuster_level) OVER w_claim_type_policy_id AS claim_type_policy_id_min_adjuster_level,
        ROW_NUMBER() OVER (ORDER BY claim_date, id) AS seq_no,
        ROUND(adjuster_level - AVG(adjuster_level) OVER w_claim_type_policy_id, 2) AS adjuster_level_vs_claim_type_policy_id_avg,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY status, adjuster_id ORDER BY adjuster_level DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS adjuster_level_avg_2_prior,
        NTILE(4) OVER (ORDER BY claim_date, id) AS claim_date_quartile,
        RANK() OVER (ORDER BY claim_date, id) AS claim_date_rank,
        SUM(adjuster_level) OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjuster_level
    FROM enriched_adjuster
    WINDOW w_claim_type AS (PARTITION BY claim_type), w_status_reported_at_to_date AS (PARTITION BY status, TO_DATE(reported_at)), w_status_adjuster_id_by_adjust AS (PARTITION BY status, adjuster_id ORDER BY adjuster_level DESC), w_claim_type_policy_id AS (PARTITION BY claim_type, policy_id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE claim_date_rank <= 3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_date_quartile, policy_status ORDER BY adjuster_level_vs_status_reported_at_to_date_avg ASC, id), 3) AS adjuster_level_vs_status_reported_at_to_date_avg_2,
        LAST_VALUE(adjuster_level_rank) OVER (PARTITION BY status, adjuster_team ORDER BY adjuster_level_vs_status_reported_at_to_date_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_adjuster_level_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claim_type_policy_id_min_adjuster_level DESC, id) AS seq_no_2,
        ROUND(AVG(next2_adjuster_level) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claim_type_policy_id_min_adjuster_level DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS next2_adjuster_level_avg_3,
        FIRST_VALUE(claim_date_rank) OVER (PARTITION BY claim_date_quartile, policy_status ORDER BY adjuster_level_vs_status_reported_at_to_date_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_rank,
        NTILE(10) OVER (PARTITION BY status, adjuster_team ORDER BY adjuster_level_vs_status_reported_at_to_date_avg ASC, id) AS adjuster_level_vs_status_reported_at_to_date_avg_3,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claim_type_policy_id_min_adjuster_level DESC, id) AS claim_type_policy_id_min_adjuster_level_rank
    FROM top_per_group
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    status,
    reported_at,
    adjuster_name,
    policy_status,
    policy_product,
    next2_adjuster_level,
    adjuster_level_vs_status_reported_at_to_date_avg,
    adjuster_level_rank,
    claim_type_policy_id_avg_adjuster_level,
    claim_type_policy_id_min_adjuster_level,
    seq_no,
    adjuster_level_vs_claim_type_policy_id_avg,
    adjuster_level_avg_2_prior,
    claim_date_rank,
    running_adjuster_level,
    rows_in_policy_id,
    adjuster_level_vs_status_reported_at_to_date_avg_2,
    seq_no_2,
    next2_adjuster_level_avg_3,
    adjuster_level_vs_status_reported_at_to_date_avg_3,
    first_known_reported_at + INTERVAL 15 MINUTES AS first_known_reported_at_plus_15m
FROM with_trends
WHERE HOUR(reported_at) BETWEEN 10 AND 13
ORDER BY rows_in_policy_id, adjuster_name DESC
