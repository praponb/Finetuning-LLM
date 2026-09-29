WITH base AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug,
        COALESCE(level, 0) AS level_filled
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'F%'
      AND team IN ('Property', 'Motor', 'Travel')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_name,
        team,
        adjuster_name_slug,
        level_filled,
        ROUND(level / NULLIF(SUM(level) OVER w_level_filled, 0), 4) AS level_share_of_level_filled,
        ROUND(PERCENT_RANK() OVER (ORDER BY level_filled), 3) AS level_filled_percent_rank
    FROM base
    WINDOW w_level_filled AS (PARTITION BY level_filled)
)
SELECT *
FROM (
    SELECT level_filled, team, level_share_of_level_filled
    FROM windowed
) src
PIVOT (
    AVG(level_share_of_level_filled)
    FOR team IN ('Health', 'Property')
)
