WITH valid_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        ABS(level - 2) AS level_distance
    FROM adjusters
),
team_stats AS (
    -- Avg level_distance per team
    SELECT team, AVG(level_distance) AS avg_level_distance
    FROM valid_adjusters
    GROUP BY team
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_level_distance,
        ROUND(s.level_distance - g.avg_level_distance, 2) AS level_distance_minus_avg
    FROM valid_adjusters s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level_distance > g.avg_level_distance
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY level_distance, level ORDER BY level_distance_minus_avg), 3) AS level_distance_minus_avg_percent_rank,
        LAG(level_distance_minus_avg, 3) OVER (PARTITION BY level_distance, level ORDER BY level_distance_minus_avg, id) AS prev3_level_distance_minus_avg,
        RANK() OVER w_level_distance_level_by_leve_2 AS level_distance_minus_avg_rank
    FROM outliers
    WINDOW w_level_distance_level_by_leve_2 AS (PARTITION BY level_distance, level ORDER BY level_distance_minus_avg, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, prev3_level_distance_minus_avg, level_distance_minus_avg, level_distance_minus_avg_percent_rank
    FROM windowed
) src
UNPIVOT (
    amount FOR field IN (prev3_level_distance_minus_avg, level_distance_minus_avg, level_distance_minus_avg_percent_rank)
) unp
