WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM claims
    WHERE adjuster_id >= 3
      AND claim_date BETWEEN DATE '2024-04-14' AND DATE '2025-11-29'
),
summary AS (
    -- Group-level totals
    SELECT
        policy_id,
        SUM(approved_amount) AS total_approved_amount,
        ROUND(100 * COUNT(CASE WHEN status IN ('APPROVED', 'REJECTED') THEN 1 END) / COUNT(*), 1) AS pct_status_in_approved_rejected,
        ROUND(100 * COUNT(CASE WHEN status = 'IN_REVIEW' THEN 1 END) / COUNT(*), 1) AS pct_status_in_review
    FROM recent_claims
    WHERE approved_amount BETWEEN 0.0 AND 25402.3
    GROUP BY policy_id
    HAVING SUM(approved_amount) > 0.0
),
with_parent_policy AS (
    -- Join policies
    SELECT s.*, p.holder_id AS policy_holder_id, p.end_date AS policy_end_date
    FROM summary s
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    policy_id,
    total_approved_amount,
    pct_status_in_approved_rejected,
    pct_status_in_review,
    FLOOR(total_approved_amount / 50000) * 50000 AS total_approved_amount_bucket,
    FLOOR(total_approved_amount / 50000) * 50000 AS total_approved_amount_bucket_2
FROM with_parent_policy
ORDER BY total_approved_amount_bucket DESC, total_approved_amount
