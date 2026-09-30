WITH recent AS (
    -- Scope the adjusters rows
    SELECT *
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 5
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        team,
        level,
        ROUND(level - AVG(level) OVER w_level_team, 2) AS level_vs_level_team_avg,
        ROUND(AVG(level) OVER (ORDER BY level DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS level_avg_1_prior,
        LEAD(level) OVER (ORDER BY level DESC, id) AS next_level,
        ROUND(level / NULLIF(SUM(level) OVER w_level_team, 0), 4) AS level_share_of_level_team,
        RANK() OVER w_all_by_level_ties AS level_rank,
        NTILE(3) OVER w_all_by_level AS level_tercile
    FROM recent
    WINDOW w_level_team AS (PARTITION BY level, team), w_all_by_level_ties AS (ORDER BY level DESC), w_all_by_level AS (ORDER BY level DESC, id)
),
team_stats AS (
    -- Avg level_tercile per team
    SELECT team, AVG(level_tercile) AS avg_level_tercile
    FROM windowed
    GROUP BY team
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_level_tercile,
        ROUND(s.level_tercile - g.avg_level_tercile, 2) AS level_tercile_minus_avg
    FROM windowed s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level_tercile > g.avg_level_tercile
),
team_stats_2 AS (
    -- Max level_avg_1_prior per team
    SELECT team, MAX(level_avg_1_prior) AS max_level_avg_1_prior
    FROM above_average
    GROUP BY team
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_level_avg_1_prior,
        ROUND(s.level_avg_1_prior - g.max_level_avg_1_prior, 2) AS level_avg_1_prior_minus_max
    FROM above_average s
    JOIN team_stats_2 g ON g.team = s.team
    WHERE s.level_avg_1_prior = g.max_level_avg_1_prior
),
grouped AS (
    -- Group-level totals
    SELECT
        level,
        ARRAY_SORT(COLLECT_SET(team)) AS team_list,
        COUNT(*) FILTER (WHERE team <> 'Health') AS n_team_health
    FROM compared
    GROUP BY 1
)
-- Final projection
SELECT
    level,
    team_list,
    n_team_health,
    COALESCE(n_team_health, 0) AS n_team_health_filled,
    FLOOR(n_team_health / 2) * 2 AS n_team_health_bucket
FROM grouped
