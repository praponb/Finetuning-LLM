WITH scoped_claims AS (
    -- Scope the claims rows
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
        ABS(approved_amount - 18478.3) AS approved_amount_distance
    FROM claims
    WHERE adjuster_id >= 2
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM scoped_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_date >= DATE '2024-06-27'
              AND c.policy_id <= 28
        )
),
per_group AS (
    -- Summarise per group
    SELECT
        policy_id,
        ROUND(100 * COUNT(CASE WHEN claim_type = 'delay' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_delay,
        SUM(CASE WHEN status IS NOT NULL THEN approved_amount_distance ELSE 0 END) AS approved_amount_distance_status_is_not_null,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        SUM(approved_amount) AS total_approved_amount,
        MAX(claim_date) AS last_claim_date
    FROM with_activity
    GROUP BY policy_id
    HAVING SUM(approved_amount) > 0.0
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM per_group s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.reported_at < TIMESTAMP '2025-02-12 12:00:00'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT policy_id, metric_name, metric_value
FROM (
    SELECT policy_id, approved_amount_distance_status_is_not_null, total_approved_amount, pct_claim_type_delay
    FROM qualified
) src
UNPIVOT (
    metric_value FOR metric_name IN (approved_amount_distance_status_is_not_null, total_approved_amount, pct_claim_type_delay)
) unp
