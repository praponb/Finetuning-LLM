WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CASE WHEN team IN ('Motor', 'Travel') THEN 'group_a' WHEN team = 'Property' THEN 'group_b' ELSE 'other' END AS team_group,
        SUBSTRING(TRIM(team), 1, 3) AS team_prefix
    FROM adjusters
    WHERE team IN ('Health', 'Travel', 'Property')
      AND level > 2
),
scored AS (
    -- Derive helper columns
    SELECT *, COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM base
)
-- Subtotals with ROLLUP
SELECT
    team_group,
    level,
    team_or_unknown,
    ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
    MAX(level) AS max_level,
    COUNT(*) AS cnt
FROM scored
GROUP BY ROLLUP(team_group, level, team_or_unknown)
