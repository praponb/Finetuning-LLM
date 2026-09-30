WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT_WS('-', adjuster_name, team) AS adjuster_name_team_label
    FROM adjusters
    WHERE level < 3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(level - AVG(level) OVER (PARTITION BY level, team), 2) AS level_vs_level_team_avg,
        ROW_NUMBER() OVER w_team_level_by_level AS seq_no,
        NTILE(10) OVER w_level_by_level AS level_decile,
        MIN(level) OVER (PARTITION BY level, team) AS level_team_min_level,
        ROUND(AVG(level) OVER (PARTITION BY team, level ORDER BY level DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS level_avg_7,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY team, level ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level
    FROM valid
    WINDOW w_team_level_by_level AS (PARTITION BY team, level ORDER BY level, id), w_level_by_level AS (PARTITION BY level ORDER BY level, id)
),
team_stats AS (
    -- Avg level_team_min_level per team
    SELECT team, AVG(level_team_min_level) AS avg_level_team_min_level
    FROM with_windows
    GROUP BY team
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_level_team_min_level,
        ROUND(s.level_team_min_level - g.avg_level_team_min_level, 2) AS level_team_min_level_minus_avg
    FROM with_windows s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level_team_min_level > g.avg_level_team_min_level
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN avg_level_team_min_level >= 30.4 THEN 'HIGH' WHEN avg_level_team_min_level >= 13.2 THEN 'MEDIUM' ELSE 'LOW' END AS avg_level_team_min_level_tier,
        level,
        first_known_level,
        MAX(first_known_level) AS max_first_known_level,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        SUM(CASE WHEN adjuster_name IS NOT NULL THEN level_team_min_level ELSE 0 END) AS level_team_min_level_adjuster_name_is_not_null,
        ROUND(AVG(level), 2) AS avg_level
    FROM outliers
    GROUP BY CASE WHEN avg_level_team_min_level >= 30.4 THEN 'HIGH' WHEN avg_level_team_min_level >= 13.2 THEN 'MEDIUM' ELSE 'LOW' END, level, first_known_level
)
SELECT avg_level_team_min_level_tier, level, measure, reading
FROM (
    SELECT avg_level_team_min_level_tier, level, first_known_level, max_first_known_level
    FROM per_group
) src
UNPIVOT (
    reading FOR measure IN (first_known_level, max_first_known_level)
) unp
ORDER BY avg_level_team_min_level_tier, level, measure
