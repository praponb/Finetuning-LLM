WITH recent AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE level > 1
      AND LOWER(adjuster_name) LIKE '%an%'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(level / NULLIF(SUM(level) OVER w_team_level, 0), 4) AS level_share_of_team_level,
        MAX(level) OVER (PARTITION BY team ORDER BY level, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS level_max_4,
        DENSE_RANK() OVER (ORDER BY level ASC NULLS LAST, id) AS level_dense_rank,
        NTH_VALUE(level, 2) OVER (ORDER BY level DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        FIRST_VALUE(level) IGNORE NULLS OVER (ORDER BY level ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        MAX(level) OVER (PARTITION BY team, level) AS team_level_max_level,
        ROUND(PERCENT_RANK() OVER (ORDER BY level ASC NULLS LAST), 3) AS level_percent_rank,
        COUNT(*) OVER (PARTITION BY team, level) AS rows_in_team_level,
        DENSE_RANK() OVER (ORDER BY level ASC NULLS LAST) AS level_dense_rank_2,
        ROUND(level - AVG(level) OVER (PARTITION BY team, level), 2) AS level_vs_team_level_avg,
        RANK() OVER w_team_by_level AS level_rank,
        ROUND(AVG(level) OVER (ORDER BY level ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS level_avg_2,
        LEAD(level, 2, 0) OVER (PARTITION BY level, team ORDER BY level DESC, id) AS next2_level
    FROM recent
    WINDOW w_team_level AS (PARTITION BY team, level), w_team_by_level AS (PARTITION BY team ORDER BY level, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        level,
        level_share_of_team_level,
        second_level,
        first_known_level,
        team_level_max_level,
        level_percent_rank,
        rows_in_team_level,
        level_dense_rank_2,
        level_vs_team_level_avg,
        level_rank,
        level_avg_2,
        next2_level,
        level_percent_rank - LAG(level_percent_rank) OVER w_team_level_dense_rank_by_row AS level_percent_rank_change,
        SIZE(COLLECT_SET(team) OVER (PARTITION BY level_dense_rank_2, second_level)) AS distinct_team_in_level_dense_rank_2_second_level,
        level_max_4 - LAG(level_max_4) OVER (PARTITION BY team, level_dense_rank ORDER BY rows_in_team_level DESC, id) AS level_max_4_change,
        COUNT(*) OVER w_level_dense_rank_2_second_le AS rows_in_level_dense_rank_2_second_level
    FROM sequenced
    WINDOW w_team_level_dense_rank_by_row AS (PARTITION BY team, level_dense_rank ORDER BY rows_in_team_level DESC, id), w_level_dense_rank_2_second_le AS (PARTITION BY level_dense_rank_2, second_level)
)
SELECT
    id,
    adjuster_name,
    level,
    level_share_of_team_level,
    second_level,
    first_known_level,
    team_level_max_level,
    rows_in_team_level,
    level_dense_rank_2,
    level_vs_team_level_avg,
    next2_level,
    level_percent_rank_change,
    distinct_team_in_level_dense_rank_2_second_level,
    level_max_4_change,
    rows_in_level_dense_rank_2_second_level,
    ROUND(level_percent_rank * 0.9, 2) AS level_percent_rank_adj
FROM windowed
ORDER BY level_vs_team_level_avg DESC, second_level NULLS LAST, id
LIMIT 20
