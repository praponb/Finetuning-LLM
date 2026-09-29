WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CASE WHEN claim_type IN ('theft', 'flood') THEN 'group_a' WHEN claim_type = 'medical' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label
    FROM claims
),
unique_rows AS (
    -- Keep the latest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claim_type_group,
        reported_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id DESC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
unique_rows_2 AS (
    -- Keep the latest row per claim_type, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claim_type_group,
        reported_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_id ORDER BY claim_date DESC, id DESC) AS row_num
        FROM unique_rows
    ) ranked
    WHERE row_num = 1
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claim_type_group,
        reported_at_label,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h
    FROM unique_rows_2
    WHERE reported_at >= TIMESTAMP '2025-01-19 12:00:00'
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        DATE_TRUNC('MONTH', reported_at_plus_2h) AS reported_at_plus_2h_month,
        ROUND(AVG(days_since_claim_date), 2) AS avg_days_since_claim_date,
        COUNT(DISTINCT claim_type_group) AS distinct_claim_type_groups,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount
    FROM annotated
    GROUP BY DATE_TRUNC('MONTH', reported_at), DATE_TRUNC('MONTH', reported_at_plus_2h)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM stats
    WHERE n_rows < (SELECT MAX(n_rows) FROM stats)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        reported_at_month,
        reported_at_plus_2h_month,
        avg_days_since_claim_date,
        distinct_claim_type_groups,
        n_rows,
        median_claimed_amount,
        LAG(avg_days_since_claim_date, 3) OVER (PARTITION BY distinct_claim_type_groups ORDER BY n_rows DESC, reported_at_month, reported_at_plus_2h_month) AS prev3_avg_days_since_claim_date,
        NTH_VALUE(distinct_claim_type_groups, 2) OVER (PARTITION BY distinct_claim_type_groups ORDER BY n_rows DESC, reported_at_month, reported_at_plus_2h_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_claim_type_groups,
        FIRST_VALUE(median_claimed_amount) OVER (PARTITION BY distinct_claim_type_groups ORDER BY n_rows DESC, reported_at_month, reported_at_plus_2h_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_median_claimed_amount
    FROM without_matches
)
SELECT
    reported_at_month,
    reported_at_plus_2h_month,
    avg_days_since_claim_date,
    n_rows,
    median_claimed_amount,
    prev3_avg_days_since_claim_date,
    second_distinct_claim_type_groups,
    first_median_claimed_amount,
    ROUND(median_claimed_amount * 1.07, 2) AS median_claimed_amount_adj,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket
FROM ranked
WHERE median_claimed_amount >= 12575.0
ORDER BY avg_days_since_claim_date DESC NULLS FIRST
