WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug,
        CASE WHEN team IN ('Property', 'Health') THEN 'group_a' WHEN team = 'Travel' THEN 'group_b' ELSE 'other' END AS team_group,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM adjusters
    WHERE team IN ('Health', 'Travel', 'Property')
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.claim_type = 'theft'
              AND c.policy_id >= 18
        )
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_name,
        s.team,
        s.level,
        c.id AS claim_id,
        c.claim_type,
        c.status AS claim_status,
        c.policy_id AS claim_policy_id,
        c.reported_at AS claim_reported_at
    FROM screened s
    JOIN claims c ON c.adjuster_id = s.id
)
SELECT
    id,
    adjuster_name,
    team,
    level,
    claim_id,
    claim_type,
    claim_policy_id,
    claim_reported_at,
    CONCAT(adjuster_name, '#', CAST(id AS STRING)) AS adjuster_name_ref
FROM detail_rows_claims
