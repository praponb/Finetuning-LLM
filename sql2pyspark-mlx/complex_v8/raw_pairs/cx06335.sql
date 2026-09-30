WITH prepared_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(adjuster_name), 1, 3) AS adjuster_name_prefix
    FROM adjusters
    WHERE (LOWER(adjuster_name) LIKE '%ra%' OR team IS NOT NULL)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        team,
        adjuster_name_prefix,
        ROUND(AVG(level) OVER (PARTITION BY level, team ORDER BY level, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS level_avg_2,
        ROUND(level / NULLIF(SUM(level) OVER w_level_team, 0), 4) AS level_share_of_level_team,
        ROUND(PERCENT_RANK() OVER w_all_by_level_ties, 3) AS level_percent_rank,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY team, level ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        ROUND(level - AVG(level) OVER (PARTITION BY level, team), 2) AS level_vs_level_team_avg,
        ROUND(AVG(level) OVER (ORDER BY level DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS level_avg_3,
        COUNT(*) OVER w_level_team AS rows_in_level_team,
        LAST_VALUE(level) OVER (ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_level,
        LAG(level, 2) OVER (ORDER BY level, id) AS prev2_level,
        level - LAG(level) OVER (PARTITION BY level, team ORDER BY level, id) AS level_change
    FROM prepared_adjusters
    WINDOW w_level_team AS (PARTITION BY level, team), w_all_by_level_ties AS (ORDER BY level)
)
-- Final projection
SELECT
    id,
    team,
    adjuster_name_prefix,
    level_avg_2,
    level_percent_rank,
    first_known_level,
    level_vs_level_team_avg,
    level_avg_3,
    last_level,
    level_change
FROM with_windows
