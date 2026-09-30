WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(adjuster_name), 1, 4) AS adjuster_name_prefix,
        ABS(level - 3) AS level_distance,
        CONCAT_WS(' / ', team, adjuster_name) AS team_adjuster_name_label
    FROM adjusters
    WHERE level < 3
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered s
    WHERE s.id IN (
            SELECT c.adjuster_id
            FROM claims c
        )
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
        )
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', team, adjuster_name_prefix) AS team_adjuster_name_prefix_label,
        CONCAT_WS('-', team_adjuster_name_label, adjuster_name_prefix) AS team_adjuster_name_label_adjuster_name_prefix_la,
        LENGTH(TRIM(team)) AS team_len
    FROM qualified
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.level,
        s.adjuster_name_prefix,
        s.team_adjuster_name_label,
        s.team_adjuster_name_label_adjuster_name_prefix_la,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_date,
        c.status AS claim_status,
        c.claim_type
    FROM annotated s
    JOIN claims c ON c.adjuster_id = s.id
)
SELECT
    id,
    level,
    team_adjuster_name_label,
    team_adjuster_name_label_adjuster_name_prefix_la,
    claim_id,
    claim_reported_at,
    claim_date
FROM with_children_claims
ORDER BY claim_reported_at, claim_date DESC NULLS FIRST
