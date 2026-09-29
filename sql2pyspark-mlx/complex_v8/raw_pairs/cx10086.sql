WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        reported_at,
        ROUND(approved_amount * 1.5, 2) AS approved_amount_adj,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount
    FROM claims
    WHERE claim_type = 'theft'
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT SUM(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS total_claim_claimed_amount,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS avg_claim_approved_amount,
        (SELECT ROUND(AVG(c.claimed_amount), 2) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS avg_claim_claimed_amount
    FROM prepared s
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN status IN ('PAID', 'IN_REVIEW') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group
    FROM profiled
    WHERE reported_at >= TIMESTAMP '2025-01-31 12:00:00'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        approved_amount_adj - avg_claim_claimed_amount AS approved_amount_adj_minus_avg_claim_claimed_amou,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM derived
    WHERE avg_claim_claimed_amount >= 5090.0
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        status_or_unknown,
        MIN(avg_claim_claimed_amount) AS min_avg_claim_claimed_amount,
        COUNT(*) AS row_count,
        ROUND(SUM(avg_claim_claimed_amount) / NULLIF(SUM(total_claim_claimed_amount), 0), 3) AS avg_claim_claimed_amount_to_total_claim_claimed_,
        SUM(CASE WHEN status = 'IN_REVIEW' THEN 1 ELSE 0 END) AS n_status_in_review,
        ROUND(SUM(avg_claim_approved_amount), 2) AS total_avg_claim_approved_amount
    FROM tagged
    GROUP BY DATE_TRUNC('MONTH', reported_at), status_or_unknown
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        reported_at_month,
        status_or_unknown,
        min_avg_claim_claimed_amount,
        avg_claim_claimed_amount_to_total_claim_claimed_,
        n_status_in_review,
        total_avg_claim_approved_amount,
        COUNT(*) OVER (PARTITION BY status_or_unknown, TO_DATE(reported_at_month)) AS rows_in_status_or_unknown_reported_at_month_to_d,
        MAX(n_status_in_review) OVER w_status_or_unknown_reported_a AS status_or_unknown_reported_at_month_to_date_max_,
        ROUND((UNIX_TIMESTAMP(reported_at_month) - UNIX_TIMESTAMP(LAG(reported_at_month) OVER (ORDER BY total_avg_claim_approved_amount ASC, reported_at_month, status_or_unknown))) / 60, 1) AS minutes_since_prev_reported_at_month,
        ROUND(AVG(row_count) OVER (PARTITION BY row_count, n_status_in_review ORDER BY row_count, reported_at_month, status_or_unknown ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS row_count_avg_3_prior,
        SUM(row_count) OVER (ORDER BY total_avg_claim_approved_amount ASC, reported_at_month, status_or_unknown ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS row_count_sum_1_prior
    FROM summary
    WINDOW w_status_or_unknown_reported_a AS (PARTITION BY status_or_unknown, TO_DATE(reported_at_month))
),
derived_2 AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(row_count_avg_3_prior * 1.1, 2) AS row_count_avg_3_prior_adj,
        DATE_TRUNC('HOUR', reported_at_month) AS reported_at_month_hour,
        LOWER(REGEXP_REPLACE(TRIM(status_or_unknown), ' ', '_')) AS status_or_unknown_slug
    FROM with_windows
)
SELECT
    reported_at_month,
    avg_claim_claimed_amount_to_total_claim_claimed_,
    n_status_in_review,
    status_or_unknown_reported_at_month_to_date_max_,
    minutes_since_prev_reported_at_month,
    row_count_sum_1_prior,
    row_count_avg_3_prior_adj,
    reported_at_month_hour,
    ABS(n_status_in_review - 4) AS n_status_in_review_distance,
    DATEDIFF(DATE '2025-12-31', reported_at_month) AS days_since_reported_at_month
FROM derived_2
WHERE row_count_avg_3_prior < 10.9
ORDER BY n_status_in_review DESC, row_count_sum_1_prior
