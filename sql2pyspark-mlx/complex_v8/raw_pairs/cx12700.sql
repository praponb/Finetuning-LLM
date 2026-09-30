WITH base AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(team), 1, 4) AS team_prefix,
        CONCAT(team, '#', CAST(level AS STRING)) AS team_ref
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'E%'
      AND level < 3
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM base
    WHERE level > (SELECT AVG(level) FROM base)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('|', team, team_ref) AS team_team_ref_label,
        COALESCE(level, 0) AS level_filled,
        CONCAT_WS(': ', team, team_prefix) AS team_team_prefix_label
    FROM without_matches
    WHERE team_prefix IS NOT NULL
),
tagged_2 AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_name,
        team,
        team_ref,
        team_team_ref_label,
        CONCAT_WS('-', team_team_prefix_label, team_ref) AS team_team_prefix_label_team_ref_label
    FROM tagged
)
SELECT
    id,
    team,
    team_ref,
    team_team_ref_label,
    team_team_prefix_label_team_ref_label,
    CONCAT(team, '#', CAST(id AS STRING)) AS team_ref_2,
    CONCAT_WS('-', team, adjuster_name) AS team_adjuster_name_label
FROM tagged_2
ORDER BY team_ref_2, team_team_prefix_label_team_ref_label DESC NULLS FIRST
