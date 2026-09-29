WITH prepared AS (
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
    WHERE status IN ('IN_REVIEW', 'REJECTED', 'APPROVED')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM prepared s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        adjuster_level - LAG(adjuster_level) OVER (PARTITION BY adjuster_level, status ORDER BY adjuster_level DESC NULLS FIRST, id) AS adjuster_level_change,
        NTH_VALUE(adjuster_level, 2) OVER (PARTITION BY adjuster_level ORDER BY adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        SUM(approved_amount) OVER (PARTITION BY status) AS status_sum_approved_amount,
        LAST_VALUE(adjuster_level) IGNORE NULLS OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_adjuster_level,
        NTH_VALUE(approved_amount, 2) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        SUM(approved_amount) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_sum_3,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_level ORDER BY adjuster_level, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_1_prior,
        FIRST_VALUE(approved_amount) OVER (PARTITION BY adjuster_level ORDER BY adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        ROW_NUMBER() OVER w_all_by_approved_amount AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_level, status ORDER BY adjuster_level DESC NULLS FIRST), 3) AS adjuster_level_percent_rank,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER w_status, 0), 4) AS adjuster_level_share_of_status,
        DENSE_RANK() OVER (PARTITION BY adjuster_level ORDER BY adjuster_level) AS adjuster_level_dense_rank,
        RANK() OVER w_all_by_approved_amount AS approved_amount_rank
    FROM combined_details
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id), w_status AS (PARTITION BY status)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    status,
    adjuster_name,
    adjuster_level,
    rows_in_status,
    second_adjuster_level,
    status_sum_approved_amount,
    last_known_adjuster_level,
    approved_amount_sum_3,
    approved_amount_avg_1_prior,
    first_approved_amount,
    adjuster_level_share_of_status,
    approved_amount_rank,
    ROUND(second_approved_amount / NULLIF(approved_amount_rank, 0), 3) AS second_approved_amount_per_approved_amount_rank
FROM with_trends
ORDER BY approved_amount_avg_1_prior DESC NULLS FIRST
