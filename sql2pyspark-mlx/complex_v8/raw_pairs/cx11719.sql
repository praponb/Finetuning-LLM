WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        DATE_TRUNC('DAY', reported_at) AS reported_at_day,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        CASE WHEN claim_type IN ('medical', 'collision') THEN 'group_a' WHEN claim_type = 'fire' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE approved_amount > 4868.7
      AND claimed_amount > 17759.3
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2
    FROM base s
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        reported_at_day,
        claim_type_group,
        claim_count,
        NTILE(4) OVER (PARTITION BY claim_count_2 ORDER BY approved_amount_filled, id) AS approved_amount_filled_quartile,
        ROUND(claim_count / NULLIF(SUM(claim_count) OVER (PARTITION BY adjuster_id), 0), 4) AS claim_count_share_of_adjuster_id,
        LAG(reported_at_day) OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY approved_amount_filled ASC, id) AS prev_reported_at_day
    FROM with_stats
    WINDOW w_claim_count_2_by_approved_am AS (PARTITION BY claim_count_2 ORDER BY approved_amount_filled, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', prev_reported_at_day) AS days_since_prev_reported_at_day,
        DATE_TRUNC('DAY', reported_at_day) AS reported_at_day_day,
        CONCAT(claim_type_group, '#', CAST(id AS STRING)) AS claim_type_group_ref
    FROM sequenced
    WHERE claim_count_share_of_adjuster_id < 0.7
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(claim_count) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at_day), policy_id) AS reported_at_day_month_policy_id_max_claim_count,
        SIZE(COLLECT_SET(claim_type) OVER w_reported_at_day_month_policy) AS distinct_claim_type_in_reported_at_day_month_pol,
        ROUND(claim_count / NULLIF(SUM(claim_count) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at_day), policy_id), 0), 4) AS claim_count_share_of_reported_at_day_month_polic,
        SUM(claim_count) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at_day), policy_id) AS reported_at_day_month_policy_id_sum_claim_count
    FROM derived
    WINDOW w_reported_at_day_month_policy AS (PARTITION BY DATE_TRUNC('MONTH', reported_at_day), policy_id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        reported_at_day,
        claim_type_group,
        approved_amount_filled_quartile,
        claim_count_share_of_adjuster_id,
        days_since_prev_reported_at_day,
        reported_at_day_day,
        claim_type_group_ref,
        reported_at_day_month_policy_id_max_claim_count,
        distinct_claim_type_in_reported_at_day_month_pol,
        reported_at_day_month_policy_id_sum_claim_count,
        ROUND((UNIX_TIMESTAMP(reported_at_day) - UNIX_TIMESTAMP(LAG(reported_at_day) OVER w_all_by_claim_count)) / 60, 1) AS minutes_since_prev_reported_at_day,
        ROW_NUMBER() OVER (ORDER BY claim_count, id) AS rn,
        ROUND(claim_count / NULLIF(SUM(claim_count) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at_day), claim_type_group), 0), 4) AS claim_count_share_of_reported_at_day_month_claim,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at_day), claim_type_group) AS rows_in_reported_at_day_month_claim_type_group,
        NTH_VALUE(reported_at_day_day, 2) OVER (ORDER BY claim_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_day_day,
        DENSE_RANK() OVER (ORDER BY claim_count) AS claim_count_dense_rank
    FROM ranked
    WINDOW w_all_by_claim_count AS (ORDER BY claim_count, id)
),
claim_type_group_stats AS (
    -- Avg rn per claim_type_group
    SELECT claim_type_group, AVG(rn) AS avg_rn
    FROM with_windows
    GROUP BY claim_type_group
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rn, ROUND(s.rn - g.avg_rn, 2) AS rn_minus_avg
    FROM with_windows s
    JOIN claim_type_group_stats g ON g.claim_type_group = s.claim_type_group
    WHERE s.rn > g.avg_rn
)
SELECT
    id,
    adjuster_id,
    claim_count_share_of_adjuster_id,
    reported_at_day_day,
    claim_type_group_ref,
    reported_at_day_month_policy_id_max_claim_count,
    distinct_claim_type_in_reported_at_day_month_pol,
    minutes_since_prev_reported_at_day,
    rn,
    second_reported_at_day_day,
    claim_count_dense_rank,
    avg_rn,
    rn_minus_avg + claim_count_dense_rank AS rn_minus_avg_plus_claim_count_dense_rank,
    COALESCE(claim_type_group, 'UNKNOWN') AS claim_type_group_or_unknown
FROM outliers
ORDER BY adjuster_id
