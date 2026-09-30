WITH cleaned_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len,
        COALESCE(level, 0) AS level_filled,
        COALESCE(level, 0) AS level_filled_2
    FROM adjusters
    WHERE team <> 'Travel'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(level_filled, 0) AS level_filled_filled,
        ROUND(level_filled / NULLIF(adjuster_name_len, 0), 3) AS level_filled_per_adjuster_name_len
    FROM cleaned_adjusters
),
team_stats AS (
    -- Avg level_filled_2 per team
    SELECT team, AVG(level_filled_2) AS avg_level_filled_2
    FROM tagged
    GROUP BY team
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_level_filled_2,
        ROUND(s.level_filled_2 - g.avg_level_filled_2, 2) AS level_filled_2_minus_avg
    FROM tagged s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level_filled_2 > g.avg_level_filled_2
)
SELECT
    level,
    level_filled,
    adjuster_name_len,
    COUNT(*) AS row_count,
    COUNT(DISTINCT team) AS distinct_teams,
    ROUND(STDDEV(level_filled_filled), 2) AS stddev_level_filled_filled
FROM vs_group
GROUP BY ROLLUP(level, level_filled, adjuster_name_len)
ORDER BY level NULLS LAST, level_filled NULLS LAST, adjuster_name_len NULLS LAST
