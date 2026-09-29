WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        reported_at + INTERVAL 15 MINUTES AS reported_at_plus_15m,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label
    FROM claims
    WHERE claim_date < DATE '2024-12-20'
      AND reported_at >= TIMESTAMP '2025-02-06 12:00:00'
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
        )
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, p.status AS policy_status
    FROM qualified s
    LEFT JOIN policies p ON p.id = s.policy_id
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.reported_at_plus_15m,
        s.claim_date_label,
        s.policy_status,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name,
        p.premium AS policy_premium
    FROM combined_details s
    JOIN adjusters a ON a.id = s.adjuster_id AND UPPER(TRIM(a.adjuster_name)) LIKE 'G%'
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    reported_at_plus_15m,
    claim_date_label,
    policy_status,
    adjuster_level,
    adjuster_name,
    policy_premium,
    LOWER(REGEXP_REPLACE(TRIM(policy_status), ' ', '_')) AS policy_status_slug
FROM enriched_adjuster
ORDER BY adjuster_level NULLS LAST, id DESC
