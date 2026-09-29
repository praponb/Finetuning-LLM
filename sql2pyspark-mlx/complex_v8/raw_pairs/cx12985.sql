WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM claims
    WHERE status IN ('OPEN', 'APPROVED')
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.status = 'PAID'
              AND c.approved_amount <= 18478.3
        )
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM without_matches s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE HOUR(c.reported_at) BETWEEN 8 AND 14
        )
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.policy_id >= 21
        )
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    claimed_amount,
    status,
    status_prefix,
    SUBSTRING(TRIM(status), 1, 2) AS status_prefix_2
FROM with_activity
ORDER BY status DESC, id DESC
