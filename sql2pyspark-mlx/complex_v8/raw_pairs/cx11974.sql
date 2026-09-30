WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE policy_id >= 9
      AND claim_date BETWEEN DATE '2024-03-29' AND DATE '2025-11-15'
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status <> 'IN_REVIEW'
        )
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.reported_at,
        p.end_date AS policy_end_date
    FROM qualified s
    JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    adjuster_id,
    reported_at,
    policy_end_date,
    LOWER(TRIM(claim_type)) AS claim_type_clean,
    SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix
FROM joined_policy
WHERE policy_id <= 16
ORDER BY id DESC NULLS FIRST
LIMIT 10
