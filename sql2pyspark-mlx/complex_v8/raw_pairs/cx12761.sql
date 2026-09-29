WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM claims
),
combined_details AS (
    -- Attach policy attributes
    SELECT s.*, p.status AS policy_status, p.start_date AS policy_start_date
    FROM prepared s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.start_date BETWEEN DATE '2024-03-15' AND DATE '2025-05-28'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(reported_at, 2) OVER (ORDER BY claim_date_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROUND(claim_date_dayofweek - AVG(claim_date_dayofweek) OVER (PARTITION BY adjuster_id), 2) AS claim_date_dayofweek_vs_adjuster_id_avg,
        SUM(claim_date_dayofweek) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_claim_date_dayofweek,
        NTH_VALUE(policy_start_date, 2) OVER (ORDER BY claim_date_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_start_date,
        MAX(approved_amount) OVER (ORDER BY claim_date_dayofweek, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3,
        ROUND(CUME_DIST() OVER (ORDER BY claim_date_dayofweek), 3) AS claim_date_dayofweek_cume_dist,
        ROUND(claim_date_dayofweek / NULLIF(SUM(claim_date_dayofweek) OVER (PARTITION BY adjuster_id), 0), 4) AS claim_date_dayofweek_share_of_adjuster_id,
        ROUND(PERCENT_RANK() OVER w_all_by_claim_date_dayofweek_4, 3) AS claim_date_dayofweek_percent_rank,
        MAX(claim_date_dayofweek) OVER (PARTITION BY adjuster_id) AS adjuster_id_max_claim_date_dayofweek
    FROM combined_details
    WINDOW w_all_by_claim_date_dayofweek_4 AS (ORDER BY claim_date_dayofweek, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label,
        ADD_MONTHS(policy_start_date, 6) AS policy_start_date_plus_6m,
        DATE_TRUNC('DAY', reported_at) AS reported_at_day
    FROM metrics
    WHERE MONTH(second_reported_at) IN (1, 3)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(approved_amount, 2) OVER (ORDER BY approved_amount DESC, id) AS next2_approved_amount,
        NTILE(3) OVER (PARTITION BY YEAR(policy_start_date_plus_6m) ORDER BY claim_date_dayofweek_percent_rank, id) AS claim_date_dayofweek_percent_rank_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(policy_start_date_plus_6m) ORDER BY claim_date_dayofweek_percent_rank), 3) AS claim_date_dayofweek_percent_rank_cume_dist,
        DENSE_RANK() OVER w_all_by_approved_amount_ties AS approved_amount_dense_rank
    FROM annotated
    WINDOW w_all_by_approved_amount_ties AS (ORDER BY approved_amount DESC)
)
SELECT
    id,
    policy_id,
    approved_amount,
    reported_at,
    claim_date_dayofweek,
    policy_status,
    policy_start_date,
    second_reported_at,
    claim_date_dayofweek_vs_adjuster_id_avg,
    adjuster_id_sum_claim_date_dayofweek,
    second_policy_start_date,
    approved_amount_max_3,
    claim_date_dayofweek_cume_dist,
    claim_date_dayofweek_share_of_adjuster_id,
    adjuster_id_max_claim_date_dayofweek,
    reported_at_label,
    policy_start_date_plus_6m,
    reported_at_day,
    next2_approved_amount,
    claim_date_dayofweek_percent_rank_tercile,
    claim_date_dayofweek_percent_rank_cume_dist,
    approved_amount_dense_rank,
    ABS(claim_date_dayofweek - 4) AS claim_date_dayofweek_distance
FROM ranked
ORDER BY next2_approved_amount DESC, id
LIMIT 5
