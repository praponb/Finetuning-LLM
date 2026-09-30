WITH scoped_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(adjuster_name), 1, 4) AS adjuster_name_prefix
    FROM adjusters
    WHERE adjuster_name IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_all_by_level_ties AS level_rank,
        level - LAG(level) OVER (PARTITION BY team, level ORDER BY level DESC, id) AS level_change,
        MIN(level) OVER (ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_level_so_far,
        MAX(level) OVER (ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_level_so_far,
        NTH_VALUE(level, 2) OVER (PARTITION BY level, team ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        ROUND(CUME_DIST() OVER w_level_by_level, 3) AS level_cume_dist,
        DENSE_RANK() OVER w_level_by_level_ties AS level_dense_rank,
        ROUND(level - AVG(level) OVER (PARTITION BY team, level), 2) AS level_vs_team_level_avg,
        ROW_NUMBER() OVER w_level_by_level AS rn
    FROM scoped_adjusters
    WINDOW w_all_by_level_ties AS (ORDER BY level DESC), w_level_by_level AS (PARTITION BY level ORDER BY level DESC, id), w_level_by_level_ties AS (PARTITION BY level ORDER BY level DESC)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(min_level_so_far, level_dense_rank) AS greatest_min_level_so_far_level_dense_rank
    FROM with_trends
    WHERE second_level < 3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY team) AS rows_in_team,
        FIRST_VALUE(max_level_so_far) IGNORE NULLS OVER (PARTITION BY level ORDER BY max_level_so_far ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_level_so_far,
        ROUND(AVG(max_level_so_far) OVER (PARTITION BY second_level, level ORDER BY level_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS max_level_so_far_avg_7,
        MAX(level_dense_rank) OVER (PARTITION BY team) AS team_max_level_dense_rank,
        FIRST_VALUE(second_level) OVER (ORDER BY greatest_min_level_so_far_level_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_second_level,
        ROUND(CUME_DIST() OVER (PARTITION BY level_rank, second_level ORDER BY rn), 3) AS rn_cume_dist
    FROM annotated
)
SELECT
    id,
    adjuster_name,
    team,
    level,
    adjuster_name_prefix,
    level_change,
    min_level_so_far,
    max_level_so_far,
    second_level,
    level_dense_rank,
    level_vs_team_level_avg,
    rn,
    greatest_min_level_so_far_level_dense_rank,
    first_known_max_level_so_far,
    max_level_so_far_avg_7,
    team_max_level_dense_rank,
    first_second_level,
    rn_cume_dist,
    CONCAT_WS('|', team, adjuster_name_prefix) AS team_adjuster_name_prefix_label,
    CASE WHEN level_change >= 2 THEN 'A' WHEN level_change >= -2 THEN 'B' ELSE 'C' END AS level_change_tier
FROM with_windows
ORDER BY level_dense_rank, adjuster_name DESC NULLS FIRST
