WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CASE WHEN status IN ('APPROVED', 'REJECTED') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 10 AND 16
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        status_group,
        days_since_reported_at,
        FIRST_VALUE(days_since_reported_at) IGNORE NULLS OVER (PARTITION BY status_group ORDER BY days_since_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_reported_at,
        SUM(days_since_reported_at) OVER (PARTITION BY status ORDER BY days_since_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_reported_at,
        SIZE(COLLECT_SET(policy_id) OVER w_adjuster_id) AS distinct_policy_id_in_adjuster_id,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY status_group ORDER BY days_since_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY adjuster_id)) AS distinct_claim_type_in_adjuster_id,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_claimed_amount,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        NTILE(5) OVER w_adjuster_id_status_by_report AS reported_at_quintile,
        DENSE_RANK() OVER w_status_group_by_days_since_r_3 AS days_since_reported_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY adjuster_id, status ORDER BY reported_at, id) AS reported_at_dense_rank,
        DENSE_RANK() OVER w_status_by_days_since_reporte_2 AS days_since_reported_at_dense_rank_2
    FROM filtered
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id), w_adjuster_id_status_by_report AS (PARTITION BY adjuster_id, status ORDER BY reported_at, id), w_status_group_by_days_since_r_3 AS (PARTITION BY status_group ORDER BY days_since_reported_at), w_status_by_days_since_reporte_2 AS (PARTITION BY status ORDER BY days_since_reported_at)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        days_since_reported_at,
        running_days_since_reported_at,
        second_claimed_amount,
        adjuster_id_avg_claimed_amount,
        days_since_reported_at_dense_rank,
        reported_at_dense_rank,
        days_since_reported_at_dense_rank_2,
        ROUND(second_claimed_amount * 1.1, 2) AS second_claimed_amount_adj,
        CONCAT_WS('|', status_group, claim_type) AS status_group_claim_type_label,
        ROUND(claimed_amount * 1.5, 2) AS claimed_amount_adj
    FROM metrics
    WHERE running_days_since_reported_at < 5250
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(days_since_reported_at / NULLIF(SUM(days_since_reported_at) OVER (PARTITION BY claim_type), 0), 4) AS days_since_reported_at_share_of_claim_type,
        ROUND(AVG(days_since_reported_at) OVER w_claim_type, 2) AS claim_type_avg_days_since_reported_at,
        LEAD(second_claimed_amount_adj) OVER (PARTITION BY days_since_reported_at_dense_rank, claim_type ORDER BY running_days_since_reported_at ASC, id) AS next_second_claimed_amount_adj,
        DENSE_RANK() OVER (PARTITION BY days_since_reported_at_dense_rank, claim_type ORDER BY running_days_since_reported_at ASC, id) AS running_days_since_reported_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY days_since_reported_at_dense_rank, claim_type ORDER BY running_days_since_reported_at ASC, id) AS seq_no
    FROM derived
    WINDOW w_claim_type AS (PARTITION BY claim_type)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    days_since_reported_at,
    running_days_since_reported_at,
    adjuster_id_avg_claimed_amount,
    days_since_reported_at_dense_rank,
    reported_at_dense_rank,
    days_since_reported_at_dense_rank_2,
    second_claimed_amount_adj,
    status_group_claim_type_label,
    claimed_amount_adj,
    days_since_reported_at_share_of_claim_type,
    claim_type_avg_days_since_reported_at,
    next_second_claimed_amount_adj,
    running_days_since_reported_at_dense_rank,
    seq_no,
    ABS(second_claimed_amount - 20461.6) AS second_claimed_amount_distance,
    CASE WHEN seq_no >= 6 THEN 'large' WHEN seq_no >= 5 THEN 'medium' ELSE 'small' END AS seq_no_tier
FROM with_windows
ORDER BY adjuster_id NULLS LAST, id
LIMIT 5
