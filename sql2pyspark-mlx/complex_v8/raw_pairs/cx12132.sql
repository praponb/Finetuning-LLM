WITH cleaned AS (
    -- Filter claims
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
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
    FROM claims
    WHERE status = 'REJECTED'
      AND adjuster_id IS NOT NULL
),
with_parent AS (
    -- Join adjusters
    SELECT s.*, a.level AS adjuster_level
    FROM cleaned s
    JOIN adjusters a ON a.id = s.adjuster_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_policy_id_by_adjuster_level, 3) AS adjuster_level_cume_dist,
        COUNT(*) OVER (PARTITION BY policy_id ORDER BY adjuster_level ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER w_policy_id_by_approved_amount AS approved_amount_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY adjuster_level ASC, id) AS seq_no,
        SIZE(COLLECT_SET(status) OVER w_claim_date_month) AS distinct_status_in_claim_date_month,
        NTILE(3) OVER (ORDER BY reported_at DESC, id) AS reported_at_tercile,
        ROUND(AVG(approved_amount) OVER (PARTITION BY status ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_2_prior,
        MAX(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_max_approved_amount
    FROM with_parent
    WINDOW w_policy_id_by_adjuster_level AS (PARTITION BY policy_id ORDER BY adjuster_level ASC), w_policy_id_by_approved_amount AS (PARTITION BY policy_id ORDER BY approved_amount, id), w_claim_date_month AS (PARTITION BY DATE_TRUNC('MONTH', claim_date))
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(adjuster_level_cume_dist, 2) OVER (PARTITION BY policy_id ORDER BY adjuster_level_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level_cume_dist,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY adjuster_level_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (ORDER BY reported_at_tercile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        ROUND(reported_at_tercile - AVG(reported_at_tercile) OVER w_policy_id, 2) AS reported_at_tercile_vs_policy_id_avg,
        RANK() OVER (ORDER BY reported_at_tercile ASC, id) AS reported_at_tercile_rank,
        LAST_VALUE(adjuster_level) IGNORE NULLS OVER (ORDER BY reported_at_tercile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_adjuster_level
    FROM windowed
    WINDOW w_policy_id AS (PARTITION BY policy_id)
)
-- Final projection
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
    claim_type_ref,
    adjuster_level,
    running_count,
    approved_amount_dense_rank,
    seq_no,
    distinct_status_in_claim_date_month,
    reported_at_tercile,
    approved_amount_avg_2_prior,
    claim_type_max_approved_amount,
    second_adjuster_level_cume_dist,
    last_known_claimed_amount,
    last_known_adjuster_level,
    QUARTER(claim_date) AS claim_date_quarter,
    CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
FROM metrics
ORDER BY approved_amount DESC, claimed_amount DESC, id
LIMIT 10
