WITH prepared_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(level) OVER (PARTITION BY level, team ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level,
        level - LAG(level) OVER (PARTITION BY level, team ORDER BY level DESC, id) AS level_change,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY level ORDER BY level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        COUNT(*) OVER w_level_team AS rows_in_level_team,
        ROUND(AVG(level) OVER (PARTITION BY level, team), 2) AS level_team_avg_level,
        MAX(level) OVER (PARTITION BY level ORDER BY level DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS level_max_6_prior,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY level ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level_2,
        DENSE_RANK() OVER w_level_by_level_ties AS level_dense_rank,
        LEAD(level, 3) OVER (PARTITION BY level ORDER BY level DESC, id) AS next3_level,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY level NULLS LAST) AS level_dense_rank_2,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY level DESC, id) AS rn,
        MIN(level) OVER (PARTITION BY level ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_level_so_far
    FROM prepared_adjusters
    WINDOW w_level_team AS (PARTITION BY level, team), w_level_by_level_ties AS (PARTITION BY level ORDER BY level DESC)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        level,
        last_known_level,
        rows_in_level_team,
        level_max_6_prior,
        last_known_level_2,
        level_dense_rank,
        next3_level,
        level_dense_rank_2,
        rn,
        level_dense_rank + rows_in_level_team AS level_dense_rank_plus_rows_in_level_team,
        CASE WHEN rows_in_level_team >= 13 THEN 'large' WHEN rows_in_level_team >= 6 THEN 'medium' ELSE 'small' END AS rows_in_level_team_tier,
        SUBSTRING(TRIM(team), 1, 2) AS team_prefix
    FROM metrics
)
-- Final projection
SELECT
    id,
    last_known_level,
    rows_in_level_team,
    level_max_6_prior,
    last_known_level_2,
    level_dense_rank,
    next3_level,
    level_dense_rank_2,
    rn,
    level_dense_rank_plus_rows_in_level_team,
    rows_in_level_team_tier,
    team_prefix,
    level_max_6_prior - rows_in_level_team AS level_max_6_prior_minus_rows_in_level_team,
    COALESCE(last_known_level_2, 0) AS last_known_level_2_filled
FROM derived
