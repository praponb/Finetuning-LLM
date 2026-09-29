WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        ABS(claimed_amount - 15381.6) AS claimed_amount_distance
    FROM claims
    WHERE policy_id <= 9
      AND HOUR(reported_at) BETWEEN 9 AND 12
),
policy_id_stats AS (
    -- Median approved_amount per policy_id
    SELECT policy_id, PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount
    FROM prepared
    GROUP BY policy_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount,
        ROUND(s.approved_amount - g.median_approved_amount, 2) AS approved_amount_minus_median
    FROM prepared s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.approved_amount >= g.median_approved_amount
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        status,
        adjuster_id,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(approved_amount), 2) AS total_approved_amount,
        SUM(median_approved_amount) FILTER (WHERE status = 'OPEN') AS median_approved_amount_status_open,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS n_status_is_not_null,
        ROUND(100 * COUNT(CASE WHEN status IN ('APPROVED', 'OPEN') THEN 1 END) / COUNT(*), 1) AS pct_status_in_approved_open
    FROM compared
    GROUP BY 1, 2, 3
    HAVING MAX(claimed_amount_distance) > 10000.0
)
SELECT
    claim_date_ym,
    status,
    adjuster_id,
    distinct_ids,
    total_approved_amount,
    median_approved_amount_status_open,
    n_status_is_not_null,
    pct_status_in_approved_open,
    ROUND(total_approved_amount / NULLIF(n_status_is_not_null, 0), 3) AS total_approved_amount_per_n_status_is_not_null
FROM rollup_base
WHERE adjuster_id IS NOT NULL
