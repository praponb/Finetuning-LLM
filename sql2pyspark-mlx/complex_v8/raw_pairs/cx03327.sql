WITH filtered_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE (UPPER(TRIM(adjuster_name)) LIKE 'A%' OR team = 'Travel')
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        team,
        level,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(adjuster_name)) >= 8) AS n_length_trim_adjuster_name_8,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        ROUND(AVG(level), 2) AS avg_level
    FROM filtered_adjusters
    WHERE level <= 3
    GROUP BY team, level
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM summary
    WHERE level < (SELECT MAX(level) FROM summary)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team ORDER BY avg_level DESC), 3) AS avg_level_percent_rank,
        NTH_VALUE(level, 2) OVER (PARTITION BY team ORDER BY avg_level DESC, team, level ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        LAG(avg_level, 2) OVER w_team_by_avg_level AS prev2_avg_level,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY level), 0), 4) AS level_share_of_level,
        LAG(level) OVER w_team_by_avg_level AS prev_level
    FROM screened
    WINDOW w_team_by_avg_level AS (PARTITION BY team ORDER BY avg_level DESC, team, level)
)
SELECT
    team,
    level,
    n_length_trim_adjuster_name_8,
    avg_level_percent_rank,
    second_level,
    prev2_avg_level,
    level_share_of_level,
    prev_level
FROM ranked
WHERE second_level >= 1
ORDER BY n_length_trim_adjuster_name_8, prev2_avg_level
