WITH filtered AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug
    FROM adjusters
    WHERE level < 3
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_name,
        adjuster_name_slug,
        ROUND(level / NULLIF(SUM(level) OVER w_level, 0), 4) AS level_share_of_level,
        NTH_VALUE(level, 2) OVER (PARTITION BY level ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        RANK() OVER (PARTITION BY level, team ORDER BY level DESC) AS level_rank,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY level, team), 0), 4) AS level_share_of_level_team,
        LAG(level) OVER (ORDER BY level DESC, id) AS prev_level,
        ROUND(CUME_DIST() OVER (ORDER BY level DESC), 3) AS level_cume_dist,
        FIRST_VALUE(level) OVER (PARTITION BY level, team ORDER BY level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level,
        LAG(level, 2, 0) OVER (ORDER BY level DESC, id) AS prev2_level,
        level - LAG(level) OVER (PARTITION BY level, team ORDER BY level, id) AS level_change,
        LAG(level) OVER (PARTITION BY level, team ORDER BY level NULLS LAST, id) AS prev_level_2,
        LAST_VALUE(level) IGNORE NULLS OVER (ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        level - LAG(level) OVER (PARTITION BY level, team ORDER BY level DESC, id) AS level_change_2,
        level - LAG(level) OVER (ORDER BY level, id) AS level_change_3,
        RANK() OVER (PARTITION BY level, team ORDER BY level NULLS LAST) AS level_rank_3,
        LAG(level, 1, 0) OVER (PARTITION BY level, team ORDER BY level DESC, id) AS prev_level_3
    FROM filtered
    WINDOW w_level AS (PARTITION BY level), w_level_team_by_level_ties AS (PARTITION BY level, team ORDER BY level DESC)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE level_rank <= 5
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY prev2_level) AS rows_in_prev2_level,
        FIRST_VALUE(level_rank_3) OVER (PARTITION BY prev_level_3 ORDER BY last_known_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level_rank_3,
        NTH_VALUE(prev2_level, 2) OVER (PARTITION BY prev_level_3 ORDER BY last_known_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev2_level,
        LEAD(level_change_2) OVER w_prev2_level_by_prev_level AS next_level_change_2,
        NTH_VALUE(last_known_level, 2) OVER (PARTITION BY prev2_level ORDER BY prev_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_level
    FROM leaders
    WINDOW w_prev2_level_by_prev_level AS (PARTITION BY prev2_level ORDER BY prev_level, id)
)
-- Final projection
SELECT
    id,
    adjuster_name,
    adjuster_name_slug,
    level_share_of_level,
    second_level,
    level_rank,
    level_share_of_level_team,
    level_cume_dist,
    first_level,
    prev2_level,
    prev_level_2,
    level_change_2,
    level_rank_3,
    prev_level_3,
    first_level_rank_3,
    next_level_change_2,
    second_last_known_level,
    LENGTH(TRIM(adjuster_name_slug)) AS adjuster_name_slug_len,
    ROUND(rows_in_prev2_level / NULLIF(last_known_level, 0), 3) AS rows_in_prev2_level_per_last_known_level
FROM windowed
ORDER BY id DESC
