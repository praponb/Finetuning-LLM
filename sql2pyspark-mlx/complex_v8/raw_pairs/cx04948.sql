WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE approved_amount >= 9389.3
      AND claim_date IS NOT NULL
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MAX(claim_date) AS last_claim_date,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS claim_status_count
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date,
        c.avg_claim_approved_amount,
        COALESCE(c.claim_status_count, 0) AS claim_status_count
    FROM valid s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(last_claim_date, claim_date) AS days_claim_date_to_last_claim_date
    FROM with_counts
    WHERE claim_date BETWEEN DATE '2024-04-27' AND DATE '2025-08-10'
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        last_claim_date,
        avg_claim_approved_amount,
        days_claim_date_to_last_claim_date,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        ROUND(approved_amount_filled * 1.1, 2) AS approved_amount_filled_adj,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
    FROM derived
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, adjuster_id, claim_type, days_claim_date_to_last_claim_date
    FROM annotated
) src
PIVOT (
    MIN(days_claim_date_to_last_claim_date)
    FOR claim_type IN ('medical', 'flood', 'delay')
)
ORDER BY policy_id, adjuster_id
