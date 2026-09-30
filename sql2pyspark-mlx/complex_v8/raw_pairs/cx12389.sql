WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        CONCAT_WS('-', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE claim_type IS NOT NULL
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM recent_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount >= 13635.1
        )
)
-- Final projection
SELECT DISTINCT policy_id
FROM filtered
ORDER BY policy_id DESC
