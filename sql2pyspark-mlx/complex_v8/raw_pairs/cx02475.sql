WITH recent_claims AS (
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
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter,
        CONCAT_WS(' / ', claim_type, status) AS claim_type_status_label,
        ABS(approved_amount - 15477.7) AS approved_amount_distance
    FROM claims
    WHERE status <> 'PAID'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        claim_date_quarter,
        claim_type_status_label,
        approved_amount_distance,
        LEAD(claim_date_quarter, 2) OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id) AS next2_claim_date_quarter,
        LAG(approved_amount_distance) OVER w_adjuster_id_by_claim_date AS prev_approved_amount_distance
    FROM recent_claims
    WINDOW w_adjuster_id_by_claim_date AS (PARTITION BY adjuster_id ORDER BY claim_date DESC, id)
),
joined AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_date_quarter,
        s.claim_type_status_label,
        s.approved_amount_distance,
        s.next2_claim_date_quarter,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM metrics s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_activity AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level BETWEEN 1 AND 3
              AND a.team IS NOT NULL
        )
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.claim_date_quarter,
        s.claim_type_status_label,
        s.next2_claim_date_quarter,
        s.adjuster_team,
        c.id AS claim_id,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at,
        c.claim_type
    FROM with_activity s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM line_level s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claimed_amount < 38909.4
        )
      AND s.claim_date_quarter < TIMESTAMP '2025-02-01 09:00:00'
)
-- Final projection
SELECT
    id,
    adjuster_id,
    status,
    claim_date_quarter,
    claim_type_status_label,
    next2_claim_date_quarter,
    adjuster_team,
    claim_status,
    claim_reported_at,
    claim_type
FROM filtered
WHERE adjuster_team IN ('Travel', 'Motor', 'Property')
ORDER BY claim_type NULLS LAST
