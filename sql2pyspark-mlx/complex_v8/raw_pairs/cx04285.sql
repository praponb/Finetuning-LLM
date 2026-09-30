WITH valid AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        MIN(level) OVER (PARTITION BY team, level) AS team_level_min_level,
        ROW_NUMBER() OVER w_team_level_by_level AS level_seq,
        DENSE_RANK() OVER w_team_by_level_ties AS level_dense_rank,
        level - LAG(level) OVER w_all_by_level AS level_change,
        LAG(level, 3) OVER (PARTITION BY team ORDER BY level ASC, id) AS prev3_level,
        COUNT(*) OVER (PARTITION BY team, level) AS rows_in_team_level,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team ORDER BY level ASC, id), 3) AS level_percent_rank,
        DENSE_RANK() OVER (PARTITION BY team ORDER BY level ASC, id) AS level_dense_rank_3,
        level - LAG(level) OVER w_team_by_level AS level_change_2,
        level - LAG(level) OVER w_team_level_by_level AS level_change_3,
        ROUND(AVG(level) OVER (PARTITION BY team, level), 2) AS team_level_avg_level,
        SUM(level) OVER (PARTITION BY team, level ORDER BY level DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS level_sum_6_prior,
        COUNT(*) OVER (ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(level) OVER (PARTITION BY team, level ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_level
    FROM valid
    WINDOW w_team_level_by_level AS (PARTITION BY team, level ORDER BY level DESC, id), w_team_by_level_ties AS (PARTITION BY team ORDER BY level ASC), w_all_by_level AS (ORDER BY level DESC, id), w_team_by_level AS (PARTITION BY team ORDER BY level ASC, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        level_dense_rank_3 + level_dense_rank AS level_dense_rank_3_plus_level_dense_rank,
        ABS(rows_in_team_level - 11) AS rows_in_team_level_distance,
        CASE WHEN level_dense_rank >= 8 THEN 'HIGH' WHEN level_dense_rank >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS level_dense_rank_tier
    FROM metrics
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(level_change_3 - AVG(level_change_3) OVER (PARTITION BY prev3_level, level_dense_rank_tier), 2) AS level_change_3_vs_prev3_level_level_dense_rank_t,
        LEAD(level_change_3, 2) OVER (PARTITION BY level_change_3 ORDER BY level_sum_6_prior DESC, id) AS next2_level_change_3,
        ROUND(AVG(prev3_level) OVER (PARTITION BY level_change, level_dense_rank_tier ORDER BY level_percent_rank NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS prev3_level_avg_3_prior
    FROM scored
)
-- Final projection
SELECT
    id,
    level_seq,
    level_change,
    prev3_level,
    level_percent_rank,
    level_dense_rank_3,
    level_change_2,
    team_level_avg_level,
    level_sum_6_prior,
    running_count,
    running_level,
    level_dense_rank_3_plus_level_dense_rank,
    rows_in_team_level_distance,
    level_dense_rank_tier,
    next2_level_change_3,
    prev3_level_avg_3_prior
FROM sequenced
ORDER BY level_dense_rank_3 DESC, level_dense_rank_3_plus_level_dense_rank, id
LIMIT 10
