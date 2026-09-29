WITH scoped AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
    FROM adjusters
    WHERE level >= 2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_team_level AS rows_in_team_level,
        MIN(level) OVER (PARTITION BY team, level) AS team_level_min_level,
        COUNT(*) OVER (PARTITION BY level, team) AS rows_in_level_team,
        MAX(level) OVER (PARTITION BY team, level) AS team_level_max_level,
        ROW_NUMBER() OVER (ORDER BY level, id) AS rn,
        ROUND(AVG(level) OVER (ORDER BY level, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS level_avg_7,
        DENSE_RANK() OVER (ORDER BY level, id) AS level_dense_rank,
        LAST_VALUE(level) OVER (PARTITION BY level, team ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_level,
        DENSE_RANK() OVER (PARTITION BY team ORDER BY level NULLS LAST) AS level_dense_rank_2
    FROM scoped
    WINDOW w_team_level AS (PARTITION BY team, level)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_slug,
        rows_in_team_level,
        team_level_min_level,
        rows_in_level_team,
        team_level_max_level,
        rn,
        level_avg_7,
        last_level,
        level_dense_rank_2
    FROM windowed
    WHERE level_dense_rank <= 5
      AND rn < 8
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_slug,
        rows_in_team_level,
        team_level_min_level,
        rows_in_level_team,
        team_level_max_level,
        level_dense_rank_2,
        COUNT(*) OVER w_level_last_level AS rows_in_level_last_level,
        SUM(team_level_min_level) OVER (PARTITION BY level_dense_rank_2 ORDER BY level_avg_7 DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS team_level_min_level_sum_1_prior,
        SUM(level_dense_rank_2) OVER (PARTITION BY level, last_level) AS level_last_level_sum_level_dense_rank_2,
        NTH_VALUE(team_level_min_level, 2) OVER (ORDER BY team_level_min_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_team_level_min_level,
        NTILE(10) OVER w_all_by_team_level_min_level_2 AS team_level_min_level_decile
    FROM top_per_group
    WINDOW w_level_last_level AS (PARTITION BY level, last_level), w_all_by_team_level_min_level_2 AS (ORDER BY team_level_min_level, id)
)
-- Final projection
SELECT
    id,
    adjuster_name,
    team,
    level,
    team_slug,
    rows_in_team_level,
    team_level_min_level,
    rows_in_level_team,
    team_level_max_level,
    rows_in_level_last_level,
    level_last_level_sum_level_dense_rank_2,
    second_team_level_min_level,
    team_level_min_level_decile,
    COALESCE(team, 'UNKNOWN') AS team_or_unknown,
    ROUND(rows_in_level_team / NULLIF(rows_in_level_last_level, 0), 3) AS rows_in_level_team_per_rows_in_level_last_level
FROM metrics
ORDER BY level, team_level_min_level_decile NULLS LAST
