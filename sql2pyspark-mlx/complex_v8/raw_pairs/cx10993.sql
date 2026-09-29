WITH recent AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE policy_id <= 22
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY reported_at DESC), 3) AS reported_at_percent_rank,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_3,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY reported_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS claimed_amount_max_3,
        LAG(reported_at) OVER (ORDER BY approved_amount DESC, id) AS prev_reported_at,
        COUNT(*) OVER w_policy_id AS rows_in_policy_id,
        LAG(approved_amount) OVER w_all_by_approved_amount AS prev_approved_amount,
        LAG(approved_amount, 1, 0) OVER w_all_by_approved_amount AS prev_approved_amount_2,
        MIN(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_min_approved_amount,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4,
        LAG(claimed_amount, 3, 0) OVER (PARTITION BY adjuster_id ORDER BY reported_at DESC, id) AS prev3_claimed_amount,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        NTILE(3) OVER (PARTITION BY status ORDER BY approved_amount NULLS LAST, id) AS approved_amount_tercile,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_id), 0), 4) AS claimed_amount_share_of_policy_id
    FROM recent
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    reported_at,
    reported_at_percent_rank,
    approved_amount_avg_3,
    claimed_amount_max_3,
    rows_in_policy_id,
    prev_approved_amount,
    prev_approved_amount_2,
    approved_amount_avg_4,
    prev3_claimed_amount,
    running_claimed_amount,
    reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h
FROM with_windows
ORDER BY claimed_amount NULLS LAST
