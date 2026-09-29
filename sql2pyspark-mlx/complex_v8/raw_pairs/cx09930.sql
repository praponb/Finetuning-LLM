WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        INITCAP(TRIM(team)) AS team_clean,
        LOWER(TRIM(team)) AS team_clean_2,
        CASE WHEN team IN ('Motor', 'Travel') THEN 'group_a' WHEN team = 'Health' THEN 'group_b' ELSE 'other' END AS team_group
    FROM adjusters
    WHERE team = 'Property'
      AND UPPER(TRIM(adjuster_name)) LIKE 'G%'
),
team_stats AS (
    -- Avg level per team
    SELECT team, AVG(level) AS avg_level
    FROM scoped
    GROUP BY team
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_level, ROUND(s.level - g.avg_level, 2) AS level_minus_avg
    FROM scoped s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level > g.avg_level
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_group,
        level_minus_avg,
        DENSE_RANK() OVER (PARTITION BY team ORDER BY avg_level NULLS LAST, id) AS avg_level_dense_rank,
        SUM(avg_level) OVER w_team_group_team AS team_group_team_sum_avg_level,
        ROUND(avg_level / NULLIF(SUM(avg_level) OVER (PARTITION BY team_group, team), 0), 4) AS avg_level_share_of_team_group_team,
        RANK() OVER (PARTITION BY team ORDER BY avg_level NULLS LAST) AS avg_level_rank
    FROM vs_group
    WINDOW w_team_group_team AS (PARTITION BY team_group, team)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY level, id) AS level_dense_rank,
        ROUND(AVG(avg_level_dense_rank) OVER (PARTITION BY level), 2) AS level_avg_avg_level_dense_rank,
        ROUND(CUME_DIST() OVER w_level_by_level_ties, 3) AS level_cume_dist,
        level_minus_avg - LAG(level_minus_avg) OVER (PARTITION BY level ORDER BY level, id) AS level_minus_avg_change,
        level - LAG(level) OVER (PARTITION BY level ORDER BY level, id) AS level_change
    FROM with_trends
    WINDOW w_level_by_level AS (PARTITION BY level ORDER BY level, id), w_level_by_level_ties AS (PARTITION BY level ORDER BY level)
)
-- Subtotals with GROUPING SETS
SELECT
    level_dense_rank,
    team_group,
    level,
    SUM(CASE WHEN LENGTH(TRIM(adjuster_name)) >= 7 THEN avg_level_rank ELSE 0 END) AS avg_level_rank_length_trim_adjuster_name_7,
    COUNT(*) AS row_count
FROM ranked
GROUP BY GROUPING SETS ((level_dense_rank, team_group, level), (level_dense_rank), ())
