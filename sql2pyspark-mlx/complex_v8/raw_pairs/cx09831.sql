WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        UPPER(TRIM(adjuster_name)) AS adjuster_name_clean
    FROM adjusters
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(level) OVER (PARTITION BY team ORDER BY level DESC, id) AS prev_level,
        ROUND(level - AVG(level) OVER (PARTITION BY team), 2) AS level_vs_team_avg,
        ROUND(AVG(level) OVER (PARTITION BY level), 2) AS level_avg_level,
        level - LAG(level) OVER (PARTITION BY level, team ORDER BY level ASC, id) AS level_change,
        ROUND(level / NULLIF(SUM(level) OVER w_team, 0), 4) AS level_share_of_team,
        DENSE_RANK() OVER w_level_team_by_level_ties AS level_dense_rank,
        MIN(level) OVER (PARTITION BY team) AS team_min_level,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY level DESC, id) AS seq_no
    FROM scoped
    WINDOW w_team AS (PARTITION BY team), w_level_team_by_level_ties AS (PARTITION BY level, team ORDER BY level ASC)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_clean,
        prev_level,
        level_vs_team_avg,
        level_avg_level,
        level_change,
        level_share_of_team,
        level_dense_rank,
        team_min_level
    FROM ranked
    WHERE seq_no = 1
)
SELECT
    id,
    level,
    adjuster_name_clean,
    prev_level,
    level_vs_team_avg,
    level_avg_level,
    level_change,
    level_share_of_team,
    level_dense_rank,
    team_min_level,
    LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
FROM top_rows
ORDER BY adjuster_name_len DESC, level_share_of_team
