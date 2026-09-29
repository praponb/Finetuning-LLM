WITH cleaned_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(adjuster_name), 1, 2) AS adjuster_name_prefix,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown_2
    FROM adjusters
    WHERE team NOT IN ('Property', 'Travel')
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned_adjusters
    WHERE level > (SELECT AVG(level) FROM cleaned_adjusters)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_name,
        level,
        adjuster_name_prefix,
        team_or_unknown,
        team_or_unknown_2,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY level), 0), 4) AS level_share_of_level,
        ROUND(CUME_DIST() OVER (PARTITION BY team_or_unknown ORDER BY level DESC), 3) AS level_cume_dist,
        COUNT(*) OVER w_level AS rows_in_level
    FROM screened
    WINDOW w_level AS (PARTITION BY level)
)
SELECT
    team_or_unknown_2,
    team_or_unknown,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
    COUNT(*) AS row_count
FROM with_trends
GROUP BY CUBE(team_or_unknown_2, team_or_unknown)
