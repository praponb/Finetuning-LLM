WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST, id) AS claimed_amount_tercile,
        ROUND(AVG(claimed_amount) OVER w_claim_date_month, 2) AS claim_date_month_avg_claimed_amount,
        MAX(approved_amount) OVER (PARTITION BY status ORDER BY reported_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_max_6_prior,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY MONTH(claim_date)), 0), 4) AS claimed_amount_share_of_claim_date_month,
        LAST_VALUE(reported_at) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST, id) AS approved_amount_change,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY MONTH(claim_date)), 2) AS claimed_amount_vs_claim_date_month_avg,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST, id))) / 60, 1) AS minutes_since_prev_reported_at,
        ROUND(approved_amount - AVG(approved_amount) OVER w_claim_date_month, 2) AS approved_amount_vs_claim_date_month_avg,
        SIZE(COLLECT_SET(status) OVER w_claim_date_month) AS distinct_status_in_claim_date_month,
        MIN(claimed_amount) OVER (PARTITION BY MONTH(claim_date)) AS claim_date_month_min_claimed_amount,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST) AS claimed_amount_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST, id), 3) AS claimed_amount_cume_dist,
        SUM(claimed_amount) OVER w_claim_date_month AS claim_date_month_sum_claimed_amount,
        MIN(approved_amount) OVER (PARTITION BY MONTH(claim_date)) AS claim_date_month_min_approved_amount,
        LEAD(reported_at) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST, id) AS next_reported_at
    FROM filtered
    WINDOW w_claim_date_month AS (PARTITION BY MONTH(claim_date))
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(approved_amount, 2) OVER (PARTITION BY status, distinct_status_in_claim_date_month ORDER BY claimed_amount_cume_dist, id) AS prev2_approved_amount,
        SIZE(COLLECT_SET(adjuster_id) OVER w_status) AS distinct_adjuster_id_in_status,
        SUM(approved_amount_change) OVER (PARTITION BY status) AS status_sum_approved_amount_change,
        NTH_VALUE(reported_at, 2) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at
    FROM with_windows
    WINDOW w_status AS (PARTITION BY status)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    status,
    reported_at,
    claimed_amount_tercile,
    claim_date_month_avg_claimed_amount,
    claimed_amount_share_of_claim_date_month,
    last_reported_at,
    approved_amount_change,
    minutes_since_prev_reported_at,
    approved_amount_vs_claim_date_month_avg,
    claim_date_month_min_claimed_amount,
    claimed_amount_dense_rank,
    claimed_amount_cume_dist,
    claim_date_month_sum_claimed_amount,
    next_reported_at,
    prev2_approved_amount,
    status_sum_approved_amount_change,
    second_reported_at,
    CASE WHEN status IN ('REJECTED', 'APPROVED') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group,
    LENGTH(TRIM(status)) AS status_len
FROM windowed
ORDER BY status_len
