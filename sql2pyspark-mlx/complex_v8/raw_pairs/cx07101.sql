WITH valid AS (
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
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND status = 'REJECTED'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status), 2) AS claimed_amount_vs_status_avg,
        ROUND(AVG(approved_amount_bucket) OVER (PARTITION BY claim_type ORDER BY reported_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS approved_amount_bucket_avg_3,
        DENSE_RANK() OVER w_claim_type_by_reported_at_ti AS reported_at_dense_rank,
        ROUND(approved_amount_bucket / NULLIF(SUM(approved_amount_bucket) OVER (PARTITION BY status), 0), 4) AS approved_amount_bucket_share_of_status
    FROM valid
    WINDOW w_claim_type_by_reported_at_ti AS (PARTITION BY claim_type ORDER BY reported_at DESC)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(second_reported_at) AS second_reported_at_day,
        SUM(CASE WHEN status IN ('APPROVED', 'IN_REVIEW') THEN 1 ELSE 0 END) AS n_status_in_approved_in_review,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT claim_type) AS distinct_claim_types,
        MIN(approved_amount) AS min_approved_amount,
        ROUND(100 * COUNT(CASE WHEN status IN ('IN_REVIEW', 'REJECTED', 'PAID') THEN 1 END) / COUNT(*), 1) AS pct_status_in_in_review_rejected
    FROM with_windows
    GROUP BY TO_DATE(second_reported_at)
    HAVING MAX(claimed_amount_vs_status_avg) > -15000.0
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(distinct_claim_types) OVER (PARTITION BY n_rows, n_status_in_approved_in_review) AS n_rows_n_status_in_approved_in_review_max_distin,
        LEAD(second_reported_at_day) OVER (ORDER BY min_approved_amount, second_reported_at_day) AS next_second_reported_at_day,
        ROUND(AVG(distinct_claim_types) OVER w_n_rows_n_status_in_approved, 2) AS n_rows_n_status_in_approved_in_review_avg_distin
    FROM totals
    WINDOW w_n_rows_n_status_in_approved AS (PARTITION BY n_rows, n_status_in_approved_in_review)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM sequenced
    WHERE pct_status_in_in_review_rejected > (SELECT MIN(pct_status_in_in_review_rejected) FROM sequenced)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(distinct_claim_types) OVER (ORDER BY n_status_in_approved_in_review DESC, second_reported_at_day ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS distinct_claim_types_avg_4,
        ROW_NUMBER() OVER w_all_by_n_status_in_approved_2 AS seq_no,
        ROUND(AVG(distinct_claim_types) OVER (PARTITION BY YEAR(second_reported_at_day), YEAR(next_second_reported_at_day)), 2) AS second_reported_at_day_year_next_second_reported,
        SUM(distinct_claim_types) OVER (PARTITION BY n_rows, DATE_TRUNC('MONTH', second_reported_at_day) ORDER BY pct_status_in_in_review_rejected, second_reported_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_distinct_claim_types
    FROM filtered
    WINDOW w_all_by_n_status_in_approved_2 AS (ORDER BY n_status_in_approved_in_review DESC, second_reported_at_day)
)
SELECT
    second_reported_at_day,
    n_status_in_approved_in_review,
    n_rows,
    min_approved_amount,
    pct_status_in_in_review_rejected,
    n_rows_n_status_in_approved_in_review_max_distin,
    next_second_reported_at_day,
    n_rows_n_status_in_approved_in_review_avg_distin,
    seq_no,
    second_reported_at_day_year_next_second_reported,
    running_distinct_claim_types
FROM ranked
ORDER BY running_distinct_claim_types NULLS LAST, second_reported_at_day_year_next_second_reported NULLS LAST, second_reported_at_day
LIMIT 20
