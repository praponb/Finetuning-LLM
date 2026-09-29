WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        reported_at,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at
    FROM claims
    WHERE claimed_amount >= 19609.8
),
joined AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.reported_at,
        s.days_since_reported_at,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM scoped s
    JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(days_since_reported_at, 2) OVER (PARTITION BY policy_id ORDER BY policy_premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_reported_at,
        FIRST_VALUE(policy_start_date) OVER (PARTITION BY policy_status, TO_DATE(reported_at) ORDER BY days_since_reported_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_policy_start_date,
        MIN(days_since_reported_at) OVER (PARTITION BY policy_status, TO_DATE(reported_at) ORDER BY days_since_reported_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_since_reported_at_so_far,
        MAX(policy_premium) OVER (PARTITION BY adjuster_id) AS adjuster_id_max_policy_premium,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(reported_at) ORDER BY policy_premium DESC, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY policy_premium ASC, id), 3) AS policy_premium_percent_rank,
        ROUND(AVG(days_since_reported_at) OVER (PARTITION BY policy_status), 2) AS policy_status_avg_days_since_reported_at
    FROM joined
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label,
        GREATEST(policy_premium, rn) AS greatest_policy_premium_rn,
        TRUNC(policy_start_date, 'MM') AS policy_start_date_month_start
    FROM sequenced
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY adjuster_id ORDER BY policy_start_date ASC, id) AS policy_start_date_tercile,
        SUM(greatest_policy_premium_rn) OVER (PARTITION BY adjuster_id ORDER BY policy_premium_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_policy_premium_rn,
        ROUND(policy_premium_percent_rank / NULLIF(SUM(policy_premium_percent_rank) OVER (PARTITION BY policy_status, policy_id), 0), 4) AS policy_premium_percent_rank_share_of_policy_stat,
        ROUND(greatest_policy_premium_rn - AVG(greatest_policy_premium_rn) OVER (PARTITION BY policy_status, policy_id), 2) AS greatest_policy_premium_rn_vs_policy_status_poli,
        policy_premium - LAG(policy_premium) OVER (PARTITION BY MONTH(first_policy_start_date), DATE_TRUNC('MONTH', policy_start_date) ORDER BY rn ASC, id) AS policy_premium_change
    FROM scored
),
summary AS (
    -- Group-level totals
    SELECT
        policy_start_date_tercile,
        ARRAY_SORT(COLLECT_SET(policy_status)) AS policy_status_list,
        SUM(greatest_policy_premium_rn) AS total_greatest_policy_premium_rn,
        COUNT(*) AS cnt
    FROM sequenced_2
    GROUP BY 1
),
sequenced_3 AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(policy_start_date_tercile) OVER (PARTITION BY cnt, policy_start_date_tercile ORDER BY cnt NULLS LAST, policy_start_date_tercile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_policy_start_date_tercile,
        COUNT(*) OVER (PARTITION BY cnt, policy_start_date_tercile) AS rows_in_cnt_policy_start_date_tercile
    FROM summary
)
-- Final projection
SELECT
    policy_start_date_tercile,
    policy_status_list,
    cnt,
    running_policy_start_date_tercile,
    COALESCE(cnt, 0) AS cnt_filled,
    ABS(cnt - 9) AS cnt_distance
FROM sequenced_3
