WITH base_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE adjuster_name IS NOT NULL
),
latest AS (
    -- Keep the latest row per team
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY level DESC, id DESC) AS row_num
        FROM base_adjusters
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY level DESC), 3) AS level_cume_dist,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY team, level), 0), 4) AS level_share_of_team_level,
        SUM(level) OVER (PARTITION BY team, level) AS team_level_sum_level,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY team, level ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level
    FROM latest
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(level / NULLIF(level_cume_dist, 0), 3) AS level_per_level_cume_dist,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM with_trends
    WHERE team IN ('Health', 'Motor', 'Travel')
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE level > (SELECT AVG(level) FROM labeled)
)
-- Final projection
SELECT
    id,
    adjuster_name,
    level_share_of_team_level,
    team_level_sum_level,
    first_known_level,
    level_per_level_cume_dist,
    UPPER(TRIM(team_or_unknown)) AS team_or_unknown_clean
FROM screened
ORDER BY adjuster_name NULLS LAST, id
LIMIT 5
