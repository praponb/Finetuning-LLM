WITH recent AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LOWER(adjuster_name) LIKE '%va%'
),
deduped AS (
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
        FROM recent
    ) ranked
    WHERE row_num = 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        team,
        level,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY level, team ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        LEAD(level, 3, 0) OVER w_level_team_by_level AS next3_level,
        ROW_NUMBER() OVER (PARTITION BY level, team ORDER BY level, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_level_team_by_level_ties, 3) AS level_percent_rank
    FROM deduped
    WINDOW w_level_team_by_level AS (PARTITION BY level, team ORDER BY level, id), w_level_team_by_level_ties AS (PARTITION BY level, team ORDER BY level)
)
SELECT
    id,
    team,
    level,
    first_known_level,
    next3_level,
    seq_no,
    level_percent_rank,
    LENGTH(TRIM(team)) AS team_len,
    ABS(next3_level - 3) AS next3_level_distance
FROM sequenced
ORDER BY team, first_known_level
