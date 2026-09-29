WITH filtered AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE (LENGTH(TRIM(adjuster_name)) >= 4 OR level >= 1)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_name,
        team,
        level,
        ROW_NUMBER() OVER (ORDER BY level DESC, id) AS level_seq,
        COUNT(*) OVER w_level AS rows_in_level,
        MIN(level) OVER w_level AS level_min_level,
        ROUND(level - AVG(level) OVER (PARTITION BY level), 2) AS level_vs_level_avg,
        SUM(level) OVER (PARTITION BY level) AS level_sum_level,
        ROUND(PERCENT_RANK() OVER w_all_by_level_ties, 3) AS level_percent_rank,
        LAG(level, 3, 0) OVER (ORDER BY level DESC, id) AS prev3_level,
        FIRST_VALUE(level) OVER (PARTITION BY level, team ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level,
        ROUND(CUME_DIST() OVER (PARTITION BY team ORDER BY level DESC), 3) AS level_cume_dist
    FROM filtered
    WINDOW w_level AS (PARTITION BY level), w_all_by_level_ties AS (ORDER BY level DESC)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        adjuster_name,
        team,
        level,
        rows_in_level,
        level_min_level,
        level_vs_level_avg,
        level_sum_level,
        level_percent_rank,
        prev3_level,
        first_level,
        level_cume_dist
    FROM ranked
    WHERE level_seq <= 3
)
SELECT
    id,
    team,
    rows_in_level,
    level_min_level,
    level_vs_level_avg,
    level_percent_rank,
    prev3_level,
    first_level,
    LENGTH(TRIM(team)) AS team_len,
    CONCAT_WS(': ', adjuster_name, team) AS adjuster_name_team_label
FROM top_rows
ORDER BY first_level
