WITH prepared AS (
    -- Keep valid rows and derive helper columns
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
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE claim_date >= DATE '2024-08-10'
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.days_since_claim_date,
        s.claim_type_status_label,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type AS claim_claim_type
    FROM prepared s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claim_type <> 'flood'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        SUM(approved_amount) AS total_claim_approved_amount,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM line_level_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id IS NOT NULL
              AND c.claimed_amount <= 40638.4
        )
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    days_since_claim_date,
    claim_type_status_label,
    claim_id,
    claim_approved_amount,
    claim_claimed_amount,
    claim_claim_type,
    total_claim_approved_amount,
    claim_count,
    CONCAT_WS(': ', status, claim_claim_type) AS status_claim_claim_type_label,
    ABS(claimed_amount - 32796.4) AS claimed_amount_distance
FROM filtered
WHERE claim_date IS NOT NULL
ORDER BY claimed_amount_distance DESC NULLS FIRST
