WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        CONCAT_WS(': ', status, claim_type) AS status_claim_type_label,
        QUARTER(claim_date) AS claim_date_quarter
    FROM claims
    WHERE claimed_amount > 10518.4
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.reported_at,
        s.status_claim_type_label,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status
    FROM prepared_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.reported_at >= TIMESTAMP '2025-02-06 09:00:00'
),
claim_status_stats AS (
    -- Avg claim_approved_amount per claim_status
    SELECT claim_status, AVG(claim_approved_amount) AS avg_claim_approved_amount
    FROM exploded_detail_claims
    GROUP BY claim_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claim_approved_amount,
        ROUND(s.claim_approved_amount - g.avg_claim_approved_amount, 2) AS claim_approved_amount_minus_avg
    FROM exploded_detail_claims s
    JOIN claim_status_stats g ON g.claim_status = s.claim_status
    WHERE s.claim_approved_amount > g.avg_claim_approved_amount
),
latest_per_key AS (
    -- Keep the latest row per claim_type, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        reported_at,
        status_claim_type_label,
        claim_id,
        claim_reported_at,
        claim_approved_amount,
        claim_status,
        avg_claim_approved_amount,
        claim_approved_amount_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_id ORDER BY claim_approved_amount_minus_avg DESC, id DESC, claim_id DESC) AS rn
        FROM outliers
    ) ranked
    WHERE rn = 1
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(claim_reported_at, 'yyyy-MM') AS claim_reported_at_ym,
        MAX(claim_approved_amount_minus_avg) AS max_claim_approved_amount_minus_avg,
        ROUND(SUM(claim_approved_amount), 2) AS total_claim_approved_amount,
        ROUND(STDDEV(avg_claim_approved_amount), 2) AS stddev_avg_claim_approved_amount,
        ROUND(SUM(claim_approved_amount_minus_avg) / NULLIF(SUM(claim_approved_amount), 0), 3) AS claim_approved_amount_minus_avg_to_claim_approve,
        MAX(claim_reported_at) AS last_claim_reported_at
    FROM latest_per_key
    GROUP BY DATE_FORMAT(claim_reported_at, 'yyyy-MM')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(total_claim_approved_amount) OVER (PARTITION BY TO_DATE(last_claim_reported_at)) AS last_claim_reported_at_to_date_max_total_claim_a,
        ROUND((UNIX_TIMESTAMP(last_claim_reported_at) - UNIX_TIMESTAMP(LAG(last_claim_reported_at) OVER w_all_by_stddev_avg_claim_appr)) / 60, 1) AS minutes_since_prev_last_claim_reported_at,
        FIRST_VALUE(total_claim_approved_amount) IGNORE NULLS OVER (ORDER BY stddev_avg_claim_approved_amount DESC, claim_reported_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_total_claim_approved_amount
    FROM per_group
    WINDOW w_all_by_stddev_avg_claim_appr AS (ORDER BY stddev_avg_claim_approved_amount DESC, claim_reported_at_ym)
)
-- Final projection
SELECT
    claim_reported_at_ym,
    max_claim_approved_amount_minus_avg,
    total_claim_approved_amount,
    stddev_avg_claim_approved_amount,
    claim_approved_amount_minus_avg_to_claim_approve,
    last_claim_reported_at,
    last_claim_reported_at_to_date_max_total_claim_a,
    minutes_since_prev_last_claim_reported_at,
    LOWER(REGEXP_REPLACE(TRIM(claim_reported_at_ym), ' ', '_')) AS claim_reported_at_ym_slug,
    DATEDIFF(DATE '2025-06-30', last_claim_reported_at) AS days_since_last_claim_reported_at
FROM with_windows
ORDER BY stddev_avg_claim_approved_amount DESC, total_claim_approved_amount
