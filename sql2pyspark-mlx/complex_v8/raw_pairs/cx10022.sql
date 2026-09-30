WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-18 09:00:00'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_type,
        adjuster_id,
        DATE_TRUNC('MONTH', reported_at_date) AS reported_at_date_month,
        PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount,
        COUNT(*) AS row_count,
        MAX(reported_at) AS last_reported_at,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        MAX(reported_at_date) AS last_reported_at_date,
        SUM(CASE WHEN claim_type NOT IN ('medical', 'fire') THEN 1 ELSE 0 END) AS n_claim_type_not_in_medical_fi
    FROM recent_claims
    GROUP BY claim_type, adjuster_id, DATE_TRUNC('MONTH', reported_at_date)
    HAVING COUNT(*) >= 2
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.status <> 'IN_REVIEW') FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_status_count,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_date,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type IN ('delay', 'theft')) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_claim_type_count
    FROM summary s
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2
    FROM summarized_rows s
),
line_level AS (
    -- One row per claim
    SELECT
        s.claim_type,
        s.adjuster_id,
        s.reported_at_date_month,
        s.median_claimed_amount,
        s.row_count,
        s.last_reported_at,
        s.avg_claimed_amount,
        s.last_reported_at_date,
        s.n_claim_type_not_in_medical_fi,
        s.claim_status_count,
        s.last_claim_date,
        s.claim_claim_type_count,
        s.claim_count,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.reported_at AS claim_reported_at
    FROM profiled s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    claim_type,
    adjuster_id,
    reported_at_date_month,
    median_claimed_amount,
    row_count,
    avg_claimed_amount,
    last_reported_at_date,
    n_claim_type_not_in_medical_fi,
    claim_status_count,
    last_claim_date,
    claim_count,
    claim_id,
    claim_policy_id,
    claim_reported_at,
    FLOOR(row_count / 2) * 2 AS row_count_bucket,
    COALESCE(median_claimed_amount, 0.0) AS median_claimed_amount_filled
FROM line_level
ORDER BY claim_reported_at DESC
