WITH prepared_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CASE WHEN team IN ('Travel', 'Motor') THEN 'group_a' WHEN team = 'Property' THEN 'group_b' ELSE 'other' END AS team_group,
        ABS(level - 3) AS level_distance
    FROM adjusters
    WHERE (UPPER(TRIM(adjuster_name)) LIKE 'G%' OR level >= 1)
),
deduped AS (
    -- Keep the latest row per team, team_group
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_group,
        level_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team, team_group ORDER BY level_distance DESC, id DESC) AS recency_rank
        FROM prepared_adjusters
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    adjuster_name,
    team,
    level,
    team_group,
    level_distance,
    CASE WHEN team IN ('Health', 'Property') THEN 'group_a' WHEN team = 'Travel' THEN 'group_b' ELSE 'other' END AS team_group_2
FROM deduped
WHERE team = 'Health'
ORDER BY team_group_2 DESC
