WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        QUARTER(reported_at) AS reported_at_quarter
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-24 07:00:00'
      AND approved_amount <= 20843.1
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM base_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount >= 2887.3
        )
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.reported_at,
        s.reported_at_quarter,
        p.status AS policy_status
    FROM qualified s
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    reported_at,
    reported_at_quarter,
    policy_status
FROM with_details
ORDER BY claim_type DESC
