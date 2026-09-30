WITH base_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT_WS('-', adjuster_name, team) AS adjuster_name_team_label
    FROM adjusters
    WHERE level <= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        team,
        level,
        adjuster_name_team_label,
        ROUND(level - AVG(level) OVER (PARTITION BY level), 2) AS level_vs_level_avg,
        level - LAG(level) OVER (ORDER BY level DESC NULLS FIRST, id) AS level_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY level DESC NULLS FIRST), 3) AS level_percent_rank,
        LEAD(level, 3) OVER w_all_by_level AS next3_level,
        DENSE_RANK() OVER (ORDER BY level DESC NULLS FIRST) AS level_dense_rank,
        ROUND(AVG(level) OVER (PARTITION BY team, level), 2) AS team_level_avg_level,
        COUNT(*) OVER (ORDER BY level DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(level) OVER (PARTITION BY level) AS level_min_level,
        DENSE_RANK() OVER (PARTITION BY team ORDER BY level ASC) AS level_dense_rank_2,
        ROW_NUMBER() OVER w_all_by_level_2 AS seq_no,
        ROUND(level - AVG(level) OVER (PARTITION BY team, level), 2) AS level_vs_team_level_avg,
        RANK() OVER w_all_by_level_ties_2 AS level_rank,
        NTH_VALUE(level, 2) OVER (ORDER BY level DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level
    FROM base_adjusters
    WINDOW w_all_by_level AS (ORDER BY level DESC NULLS FIRST, id), w_level AS (PARTITION BY level), w_all_by_level_2 AS (ORDER BY level ASC, id), w_all_by_level_ties_2 AS (ORDER BY level ASC)
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE level_dense_rank <= 2
      AND level_vs_level_avg < 2.5
)
-- Final projection
SELECT
    id,
    team,
    level_change,
    next3_level,
    level_dense_rank,
    team_level_avg_level,
    running_count,
    level_min_level,
    level_dense_rank_2,
    level_vs_team_level_avg,
    second_level,
    ROUND(level_vs_level_avg * 1.5, 2) AS level_vs_level_avg_adj
FROM top_rows
WHERE level_rank <= 7
ORDER BY running_count, level_vs_team_level_avg NULLS LAST
