WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE (adjuster_id IS NOT NULL OR reported_at < TIMESTAMP '2025-02-18 09:00:00')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COUNT(*) OVER w_status_claim_type AS rows_in_status_claim_type,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        DENSE_RANK() OVER (ORDER BY approved_amount, id) AS approved_amount_dense_rank,
        LAG(reported_at, 2) OVER (PARTITION BY policy_id ORDER BY approved_amount, id) AS prev2_reported_at,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status, claim_type), 2) AS status_claim_type_avg_claimed_amount,
        LEAD(reported_at) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id) AS next_reported_at,
        MIN(claimed_amount) OVER w_status_claim_type AS status_claim_type_min_claimed_amount,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount, id) AS claimed_amount_change,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY policy_id ORDER BY approved_amount, id))) / 60, 1) AS minutes_since_prev_reported_at,
        SIZE(COLLECT_SET(policy_id) OVER w_status_claim_type) AS distinct_policy_id_in_status_claim_type,
        ROUND(approved_amount - AVG(approved_amount) OVER w_status_claim_type, 2) AS approved_amount_vs_status_claim_type_avg
    FROM recent
    WINDOW w_status_claim_type AS (PARTITION BY status, claim_type)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(reported_at) OVER (PARTITION BY status, MONTH(prev2_reported_at) ORDER BY prev2_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        ROW_NUMBER() OVER w_status_prev2_reported_at_mon_2 AS prev2_reported_at_seq,
        SUM(claimed_amount_change) OVER w_adjuster_id AS adjuster_id_sum_claimed_amount_change,
        DENSE_RANK() OVER (ORDER BY distinct_policy_id_in_status_claim_type DESC NULLS FIRST) AS distinct_policy_id_in_status_claim_type_dense_ra,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY status)) AS distinct_claim_type_in_status,
        ROUND(rows_in_status_claim_type - AVG(rows_in_status_claim_type) OVER (PARTITION BY status), 2) AS rows_in_status_claim_type_vs_status_avg,
        LEAD(claimed_amount) OVER (ORDER BY rows_in_status_claim_type, id) AS next_claimed_amount,
        FIRST_VALUE(next_reported_at) IGNORE NULLS OVER (ORDER BY rows_in_status_claim_type DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_reported_at
    FROM with_windows
    WINDOW w_status_prev2_reported_at_mon_2 AS (PARTITION BY status, MONTH(prev2_reported_at) ORDER BY prev2_reported_at, id), w_adjuster_id AS (PARTITION BY adjuster_id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    claimed_amount,
    status,
    last_claimed_amount,
    prev2_reported_at,
    next_reported_at,
    status_claim_type_min_claimed_amount,
    distinct_policy_id_in_status_claim_type,
    approved_amount_vs_status_claim_type_avg,
    last_reported_at,
    prev2_reported_at_seq,
    adjuster_id_sum_claimed_amount_change,
    rows_in_status_claim_type_vs_status_avg,
    next_claimed_amount,
    first_known_next_reported_at,
    DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at,
    ABS(approved_amount - 14068.2) AS approved_amount_distance
FROM with_trends
ORDER BY first_known_next_reported_at DESC
