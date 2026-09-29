WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-03-14'
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week,
        CONCAT_WS(' / ', status, reported_at_label) AS status_reported_at_label_label,
        CASE WHEN approved_amount >= 16404.6 THEN 'A' WHEN approved_amount >= 4554.8 THEN 'B' ELSE 'C' END AS approved_amount_tier
    FROM cleaned
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        claim_date_week + INTERVAL 1 DAYS AS claim_date_week_plus_1d,
        DATE_FORMAT(claim_date_week, 'yyyy-MM-dd HH:00') AS claim_date_week_label,
        CONCAT_WS(': ', reported_at_label, status) AS reported_at_label_status_label
    FROM derived
    WHERE status_reported_at_label_label IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, a.level AS adjuster_level
    FROM derived_2 s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level < 3
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) AS claim_count,
        COUNT(*) AS claim_count_2,
        MIN(reported_at) AS first_claim_reported_at
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_count_2, 0) AS claim_count_2,
        c.first_claim_reported_at
    FROM with_parent s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    approved_amount,
    status,
    status_prefix,
    claim_date_week,
    approved_amount_tier,
    claim_date_week_label,
    reported_at_label_status_label,
    claim_count,
    claim_count_2,
    first_claim_reported_at,
    DAYOFWEEK(first_claim_reported_at) AS first_claim_reported_at_dayofweek
FROM profiled
ORDER BY first_claim_reported_at, approved_amount
