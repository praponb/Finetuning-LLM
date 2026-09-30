WITH scoped AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 7
      AND level BETWEEN 1 AND 3
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        COALESCE(level, 0) AS level_filled
    FROM scoped
    WHERE level <= 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(level) OVER (PARTITION BY team_or_unknown, team ORDER BY level_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_level,
        NTH_VALUE(level, 2) OVER (PARTITION BY team_or_unknown ORDER BY level_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY level), 0), 4) AS level_share_of_level,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team_or_unknown ORDER BY level_filled DESC), 3) AS level_filled_percent_rank
    FROM tagged
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_name,
        team_or_unknown,
        level_filled,
        second_level,
        level_share_of_level,
        level_filled_percent_rank,
        CASE WHEN team_or_unknown IN ('Property', 'Health') THEN 'group_a' WHEN team_or_unknown = 'UNKNOWN' THEN 'group_b' ELSE 'other' END AS team_or_unknown_group,
        CONCAT(adjuster_name, '#', CAST(running_level AS STRING)) AS adjuster_name_ref
    FROM ranked
)
SELECT id, field, value
FROM (
    SELECT id, second_level, level_filled
    FROM labeled
) src
UNPIVOT (
    value FOR field IN (second_level AS second_level_v, level_filled AS level_filled_v)
) unp
ORDER BY id, field
