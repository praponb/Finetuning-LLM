WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount,
        LAST_DAY(claim_date) AS claim_date_month_end,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label
    FROM claims
    WHERE claimed_amount > 18478.3
      AND status <> 'PAID'
),
combined_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.reported_at,
        s.claimed_amount_per_approved_amount,
        s.claim_date_month_end,
        p.start_date AS policy_start_date
    FROM recent_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', policy_start_date) AS policy_start_date_month,
        PERCENTILE_APPROX(claimed_amount_per_approved_amount, 0.5) AS median_claimed_amount_per_approved_amount,
        ROUND(100 * COUNT(CASE WHEN status NOT IN ('OPEN', 'IN_REVIEW') THEN 1 END) / COUNT(*), 1) AS pct_status_not_in_open_in_review,
        COUNT(*) AS cnt,
        ROUND(SUM(claimed_amount_per_approved_amount), 2) AS total_claimed_amount_per_approved_amount
    FROM combined_details
    WHERE claimed_amount_per_approved_amount BETWEEN 3.0 AND 8.5
    GROUP BY DATE_TRUNC('MONTH', policy_start_date)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE total_claimed_amount_per_approved_amount > (SELECT AVG(total_claimed_amount_per_approved_amount) FROM stats)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', policy_start_date_month) AS days_since_policy_start_date_month,
        COALESCE(cnt, 0) AS cnt_filled
    FROM without_matches
    WHERE pct_status_not_in_open_in_review >= 30.0
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE median_claimed_amount_per_approved_amount > (SELECT AVG(median_claimed_amount_per_approved_amount) FROM annotated)
)
-- Final projection
SELECT
    policy_start_date_month,
    median_claimed_amount_per_approved_amount,
    pct_status_not_in_open_in_review,
    cnt,
    total_claimed_amount_per_approved_amount,
    days_since_policy_start_date_month,
    cnt + cnt_filled AS cnt_plus_cnt_filled,
    DATE_TRUNC('WEEK', policy_start_date_month) AS policy_start_date_month_week
FROM filtered
WHERE cnt <= 11
ORDER BY cnt_plus_cnt_filled NULLS LAST, policy_start_date_month
LIMIT 20
