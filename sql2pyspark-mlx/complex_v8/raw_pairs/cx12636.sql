WITH cleaned AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE level < 3
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.claimed_amount < 33975.8
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_name,
        level,
        UPPER(TRIM(team)) AS team_clean,
        SUBSTRING(TRIM(adjuster_name), 1, 4) AS adjuster_name_prefix
    FROM qualified
    WHERE team = 'Motor'
)
SELECT
    id,
    adjuster_name,
    level,
    team_clean,
    adjuster_name_prefix,
    CONCAT(adjuster_name, '#', CAST(id AS STRING)) AS adjuster_name_ref,
    COALESCE(level, 0) AS level_filled
FROM labeled
ORDER BY adjuster_name_prefix NULLS LAST, level
