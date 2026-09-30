WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(TRIM(adjuster_name)) AS adjuster_name_clean,
        CASE WHEN team IN ('Health', 'Property') THEN 'group_a' WHEN team = 'Travel' THEN 'group_b' ELSE 'other' END AS team_group
    FROM adjusters
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_level_by_level, 3) AS level_percent_rank,
        ROUND(level - AVG(level) OVER (PARTITION BY team_group), 2) AS level_vs_team_group_avg,
        FIRST_VALUE(level) OVER (ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level,
        ROUND(CUME_DIST() OVER (PARTITION BY level, team_group ORDER BY level DESC), 3) AS level_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY level NULLS LAST, id) AS rn,
        COUNT(*) OVER (PARTITION BY team, level) AS rows_in_team_level,
        MIN(level) OVER w_team_level AS team_level_min_level,
        MAX(level) OVER (PARTITION BY level, team_group ORDER BY level DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS level_max_2_prior,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY team, level), 0), 4) AS level_share_of_team_level
    FROM scoped
    WINDOW w_level_by_level AS (PARTITION BY level ORDER BY level NULLS LAST, id), w_team_level AS (PARTITION BY team, level)
)
-- Final projection
SELECT
    id,
    adjuster_name,
    team,
    adjuster_name_clean,
    level_vs_team_group_avg,
    first_level,
    rn,
    rows_in_team_level,
    team_level_min_level,
    level_max_2_prior,
    level_share_of_team_level,
    COALESCE(level_max_2_prior, 0) AS level_max_2_prior_filled
FROM windowed
WHERE team_level_min_level BETWEEN 13 AND 33
