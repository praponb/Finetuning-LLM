WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT COUNT(CASE WHEN c.status = 'PAID' THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count_2
    FROM scoped_claims s
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        claim_count,
        claim_status_count,
        claim_count_2,
        reported_at + INTERVAL 30 MINUTES AS reported_at_plus_30m,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week,
        TRUNC(claim_date, 'MM') AS claim_date_month_start
    FROM with_counts
    WHERE claimed_amount BETWEEN 3192.1 AND 45826.9
)
-- Subtotals with GROUPING SETS
SELECT
    adjuster_id,
    claim_count,
    GROUPING(adjuster_id) AS is_total_adjuster_id,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount
FROM labeled
GROUP BY GROUPING SETS ((adjuster_id, claim_count), (adjuster_id), ())
ORDER BY adjuster_id NULLS LAST, claim_count NULLS LAST
