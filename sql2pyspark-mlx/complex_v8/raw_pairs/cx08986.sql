WITH cleaned AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(adjuster_name), 1, 4) AS adjuster_name_prefix,
        ABS(level - 3) AS level_distance,
        CONCAT_WS('-', team, adjuster_name) AS team_adjuster_name_label
    FROM adjusters
    WHERE LOWER(adjuster_name) LIKE '%ha%'
      AND level < 3
),
team_stats AS (
    -- Median level_distance per team
    SELECT team, PERCENTILE_APPROX(level_distance, 0.5) AS median_level_distance
    FROM cleaned
    GROUP BY team
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_level_distance,
        ROUND(s.level_distance - g.median_level_distance, 2) AS level_distance_minus_median
    FROM cleaned s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level_distance >= g.median_level_distance
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(team_adjuster_name_label)) AS team_adjuster_name_label_len
    FROM compared
)
SELECT
    team_adjuster_name_label_len,
    team,
    level_distance,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT team) AS distinct_teams
FROM annotated
GROUP BY CUBE(team_adjuster_name_label_len, team, level_distance)
ORDER BY team_adjuster_name_label_len NULLS LAST, team NULLS LAST, level_distance NULLS LAST
