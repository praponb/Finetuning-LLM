WITH recent AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug,
        CONCAT_WS('-', adjuster_name, team) AS adjuster_name_team_label
    FROM adjusters
    WHERE level >= 3
      AND UPPER(TRIM(adjuster_name)) LIKE 'A%'
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.team,
        s.adjuster_name_team_label,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.policy_id AS claim_policy_id
    FROM recent s
    LEFT JOIN claims c ON c.adjuster_id = s.id
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM line_level_claims s
    WHERE s.id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.claimed_amount > 11235.6
        )
      AND s.team = 'Property'
)
-- Final projection
SELECT
    id,
    team,
    adjuster_name_team_label,
    claim_id,
    claim_approved_amount,
    claim_policy_id,
    UPPER(TRIM(adjuster_name_team_label)) AS adjuster_name_team_label_clean,
    LENGTH(TRIM(adjuster_name_team_label)) AS adjuster_name_team_label_len
FROM with_activity
