WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        CASE WHEN status IN ('REJECTED', 'IN_REVIEW') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-19 14:00:00'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        s.reported_at,
        s.status_group,
        s.claimed_amount_minus_approved_amount,
        p.status AS policy_status
    FROM scoped_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount BETWEEN 0.0 AND 20880.4
        )
)
-- Subtotals with GROUPING SETS
SELECT
    policy_id,
    status,
    adjuster_id,
    ROUND(AVG(claimed_amount_minus_approved_amount), 2) AS avg_claimed_amount_minus_approved_amount,
    COUNT(*) AS n_rows
FROM screened
GROUP BY GROUPING SETS ((policy_id, status, adjuster_id), (policy_id), ())
ORDER BY policy_id NULLS LAST, status NULLS LAST, adjuster_id NULLS LAST
