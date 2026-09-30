WITH base_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug,
        ABS(level - 3) AS level_distance,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug_2
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'G%'
      AND level BETWEEN 1 AND 3
),
team_stats AS (
    -- Avg level per team
    SELECT team, AVG(level) AS avg_level
    FROM base_adjusters
    GROUP BY team
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_level, ROUND(s.level - g.avg_level, 2) AS level_minus_avg
    FROM base_adjusters s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level > g.avg_level
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.status IN ('REJECTED', 'APPROVED', 'PAID')
        )
      AND s.avg_level BETWEEN 2.0 AND 4.6
),
rollup_base AS (
    -- Group-level totals
    SELECT
        level_distance,
        team,
        COUNT(*) AS cnt,
        ROUND(SUM(level_minus_avg) / NULLIF(SUM(level_distance), 0), 3) AS level_minus_avg_to_level_distance_ratio,
        MIN(avg_level) AS min_avg_level,
        COUNT(DISTINCT team) AS distinct_teams
    FROM without_matches
    GROUP BY level_distance, team
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(cnt) OVER (ORDER BY min_avg_level, level_distance, team) AS next_cnt,
        ROUND(distinct_teams / NULLIF(SUM(distinct_teams) OVER (PARTITION BY cnt, team), 0), 4) AS distinct_teams_share_of_cnt_team,
        ROUND(distinct_teams - AVG(distinct_teams) OVER (PARTITION BY cnt, team), 2) AS distinct_teams_vs_cnt_team_avg,
        LAST_VALUE(level_distance) OVER (PARTITION BY distinct_teams ORDER BY min_avg_level ASC NULLS LAST, level_distance, team ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_level_distance,
        NTH_VALUE(min_avg_level, 2) OVER (ORDER BY min_avg_level, level_distance, team ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_avg_level
    FROM rollup_base
)
-- Final projection
SELECT
    level_distance,
    team,
    cnt,
    level_minus_avg_to_level_distance_ratio,
    distinct_teams,
    next_cnt,
    distinct_teams_share_of_cnt_team,
    distinct_teams_vs_cnt_team_avg,
    second_min_avg_level,
    CONCAT(team, '#', CAST(level_distance AS STRING)) AS team_ref
FROM metrics
ORDER BY distinct_teams DESC
