WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
    FROM claims
    WHERE claim_type IN ('fire', 'delay', 'medical')
      AND reported_at < TIMESTAMP '2025-02-18 09:00:00'
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.claim_date_quarter,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM scoped s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level <= 3
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id), 2) AS claimed_amount_vs_adjuster_id_avg,
        NTH_VALUE(adjuster_level, 2) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date) ORDER BY adjuster_level ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        SUM(approved_amount) OVER (PARTITION BY adjuster_level, MONTH(reported_at) ORDER BY claim_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_level)) AS distinct_policy_id_in_adjuster_level,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        LAG(reported_at) OVER w_all_by_claimed_amount AS prev_reported_at,
        ROUND(approved_amount - AVG(approved_amount) OVER w_adjuster_level, 2) AS approved_amount_vs_adjuster_level_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_level, policy_id ORDER BY claimed_amount DESC), 3) AS claimed_amount_cume_dist,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_level), 2) AS claimed_amount_vs_adjuster_level_avg,
        ROUND(CUME_DIST() OVER w_all_by_claimed_amount_ties, 3) AS claimed_amount_cume_dist_2
    FROM enriched_adjuster
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount DESC, id), w_adjuster_level AS (PARTITION BY adjuster_level), w_all_by_claimed_amount_ties AS (ORDER BY claimed_amount DESC)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        adjuster_level,
        adjuster_name,
        second_adjuster_level,
        distinct_policy_id_in_adjuster_level,
        claimed_amount_cume_dist,
        claimed_amount_vs_adjuster_level_avg,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at
    FROM windowed
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(reported_at) AS reported_at_day,
        distinct_policy_id_in_adjuster_level,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        ROUND(SUM(claimed_amount_vs_adjuster_level_avg) / NULLIF(SUM(claimed_amount), 0), 3) AS claimed_amount_vs_adjuster_level_avg_to_claimed_,
        SUM(claimed_amount) AS total_claimed_amount,
        COUNT(*) AS cnt,
        SUM(CASE WHEN LENGTH(TRIM(adjuster_name)) >= 8 THEN approved_amount ELSE 0 END) AS approved_amount_length_trim_adjuster_name_8
    FROM annotated
    GROUP BY TO_DATE(reported_at), distinct_policy_id_in_adjuster_level
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        reported_at_day,
        distinct_policy_id_in_adjuster_level,
        adjuster_name_list,
        total_claimed_amount,
        cnt,
        approved_amount_length_trim_adjuster_name_8,
        MAX(cnt) OVER (PARTITION BY MONTH(reported_at_day)) AS reported_at_day_month_max_cnt,
        MIN(cnt) OVER (PARTITION BY distinct_policy_id_in_adjuster_level ORDER BY total_claimed_amount, reported_at_day, distinct_policy_id_in_adjuster_level ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_cnt_so_far,
        COUNT(*) OVER w_reported_at_day_month AS rows_in_reported_at_day_month
    FROM per_group
    WINDOW w_reported_at_day_month AS (PARTITION BY MONTH(reported_at_day))
)
-- Final projection
SELECT
    reported_at_day,
    distinct_policy_id_in_adjuster_level,
    adjuster_name_list,
    cnt,
    approved_amount_length_trim_adjuster_name_8,
    min_cnt_so_far,
    rows_in_reported_at_day_month
FROM sequenced
ORDER BY distinct_policy_id_in_adjuster_level DESC, rows_in_reported_at_day_month DESC
