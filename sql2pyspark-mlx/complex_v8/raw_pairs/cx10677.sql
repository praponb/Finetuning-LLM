WITH filtered AS (
    -- Keep valid rows and derive helper columns
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
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date,
        DATE_ADD(claim_date, 30) AS claim_date_plus_30d
    FROM claims
    WHERE policy_id <= 24
      AND adjuster_id IS NOT NULL
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.policy_id IS NOT NULL
              AND c.reported_at < TIMESTAMP '2025-02-12 14:00:00'
        )
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.status_slug,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_type AS claim_claim_type,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at
    FROM with_activity s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(claim_reported_at, 'yyyy-MM') AS claim_reported_at_ym,
        adjuster_id,
        status,
        SUM(CASE WHEN claim_claim_type = 'collision' THEN claimed_amount ELSE 0 END) AS claimed_amount_claim_claim_type_collision,
        ROUND(SUM(claim_approved_amount) / NULLIF(SUM(claimed_amount), 0), 3) AS claim_approved_amount_to_claimed_amount_ratio,
        COUNT(*) AS row_count,
        MIN(claimed_amount) AS min_claimed_amount,
        COUNT(DISTINCT status) AS distinct_status,
        COUNT(DISTINCT claim_type) AS distinct_claim_types
    FROM exploded_detail_claims
    WHERE policy_id >= 7
    GROUP BY DATE_FORMAT(claim_reported_at, 'yyyy-MM'), adjuster_id, status
    HAVING COUNT(*) >= 2
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM totals s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status IN ('APPROVED', 'PAID', 'IN_REVIEW')
        )
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, COUNT(*) AS claim_count, COUNT(*) AS claim_count_2
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_count_2, 0) AS claim_count_2
    FROM screened s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.claim_reported_at_ym,
        s.adjuster_id,
        s.status,
        s.min_claimed_amount,
        s.distinct_claim_types,
        s.claim_count,
        s.claim_count_2,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_type,
        c.approved_amount AS claim_approved_amount,
        c.policy_id AS claim_policy_id
    FROM profiled s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND TO_DATE(c.reported_at) < DATE '2025-03-08'
)
SELECT
    claim_reported_at_ym,
    adjuster_id,
    min_claimed_amount,
    distinct_claim_types,
    claim_count,
    claim_id,
    claim_reported_at,
    claim_type,
    claim_approved_amount,
    claim_policy_id
FROM detail_rows
WHERE claim_id IS NOT NULL
ORDER BY adjuster_id DESC, claim_approved_amount DESC
