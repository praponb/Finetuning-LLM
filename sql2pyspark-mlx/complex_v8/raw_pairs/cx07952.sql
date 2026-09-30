WITH scoped_claims AS (
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
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE claim_type IN ('delay', 'theft')
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(reported_at_date, 'yyyy-MM') AS reported_at_date_ym,
        status,
        SUM(CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END) AS n_status_is_not_null,
        SUM(claimed_amount) AS total_claimed_amount,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        MAX(approved_amount) AS max_approved_amount,
        COUNT(*) FILTER (WHERE status <> 'IN_REVIEW') AS n_status_in_review
    FROM scoped_claims
    GROUP BY 1, 2
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(total_claimed_amount / NULLIF(n_status_in_review, 0), 3) AS total_claimed_amount_per_n_status_in_review,
        ABS(total_claimed_amount - 210030.0) AS total_claimed_amount_distance,
        CASE WHEN status IN ('OPEN', 'IN_REVIEW') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM summary
    WHERE total_claimed_amount >= 45085.0
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        reported_at_date_ym,
        status,
        n_status_is_not_null,
        status_list,
        max_approved_amount,
        n_status_in_review,
        ROUND(total_claimed_amount - AVG(total_claimed_amount) OVER (PARTITION BY status, status_group), 2) AS total_claimed_amount_vs_status_status_group_avg,
        DENSE_RANK() OVER (PARTITION BY status_group, n_status_in_review ORDER BY max_approved_amount, reported_at_date_ym, status) AS max_approved_amount_dense_rank
    FROM labeled
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM with_trends
    WHERE max_approved_amount_dense_rank <= 3
      AND max_approved_amount > 15000.0
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM first_rows
    WHERE n_status_in_review > (SELECT AVG(n_status_in_review) FROM first_rows)
),
flattened AS (
    -- One row per status_list_item
    SELECT
        s.reported_at_date_ym,
        s.status,
        s.n_status_is_not_null,
        s.max_approved_amount,
        s.n_status_in_review,
        s.total_claimed_amount_vs_status_status_group_avg,
        s.max_approved_amount_dense_rank,
        t.status_list_item
    FROM with_activity s
    LATERAL VIEW EXPLODE(s.status_list) t AS status_list_item
)
SELECT
    reported_at_date_ym,
    status,
    n_status_is_not_null,
    max_approved_amount,
    n_status_in_review,
    total_claimed_amount_vs_status_status_group_avg,
    max_approved_amount_dense_rank,
    status_list_item,
    COALESCE(n_status_in_review, 0) AS n_status_in_review_filled
FROM flattened
ORDER BY max_approved_amount DESC, n_status_in_review_filled
