WITH base AS (
    -- Filter claims
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
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE policy_id >= 9
      AND reported_at >= TIMESTAMP '2025-02-06 07:00:00'
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
      AND s.status <> 'IN_REVIEW'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, claimed_amount, approved_amount
    FROM qualified
) src
UNPIVOT (
    value FOR metric_name IN (claimed_amount, approved_amount)
) unp
