WITH scoped AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT(adjuster_name, '#', CAST(id AS STRING)) AS adjuster_name_ref,
        CONCAT_WS('-', adjuster_name, team) AS adjuster_name_team_label
    FROM adjusters
    WHERE level < 3
      AND team <> 'Health'
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(' / ', adjuster_name, adjuster_name_team_label) AS adjuster_name_adjuster_name_team_label_label
    FROM scoped
    WHERE team IS NOT NULL
)
SELECT
    id,
    adjuster_name,
    adjuster_name_ref,
    adjuster_name_team_label,
    adjuster_name_adjuster_name_team_label_label
FROM annotated
ORDER BY id DESC NULLS FIRST, adjuster_name_team_label DESC
