WITH prepared_claims AS (
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
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at
    FROM claims
    WHERE policy_id <= 3
      AND claimed_amount > 18364.1
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE status IN ('OPEN', 'APPROVED')) AS claim_status_count
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count
    FROM prepared_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_date, adjuster_id, status, approved_amount
    FROM profiled
) src
PIVOT (
    MAX(approved_amount)
    FOR status IN ('OPEN', 'IN_REVIEW', 'PAID', 'REJECTED')
)
ORDER BY claim_date, adjuster_id
