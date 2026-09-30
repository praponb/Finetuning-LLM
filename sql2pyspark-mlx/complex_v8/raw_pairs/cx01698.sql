WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE claim_type IN ('delay', 'theft', 'medical')
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        claimed_amount,
        approved_amount,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM recent_claims
)
-- Subtotals with CUBE
SELECT
    COALESCE(status, 'ALL') AS status,
    policy_id,
    GROUPING(status) AS is_total_status,
    COUNT(DISTINCT status) AS distinct_status,
    SUM(CASE WHEN status <> 'IN_REVIEW' THEN claimed_amount ELSE 0 END) AS claimed_amount_status_in_review,
    ROUND(100 * COUNT(CASE WHEN status = 'REJECTED' THEN 1 END) / COUNT(*), 1) AS pct_status_rejected
FROM labeled
GROUP BY CUBE(status, policy_id)
ORDER BY status NULLS LAST, policy_id NULLS LAST
