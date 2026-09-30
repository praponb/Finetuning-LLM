WITH valid_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE team IN ('Travel', 'Health', 'Motor')
      AND adjuster_name IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        level,
        COALESCE(level, 0) AS level_filled,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM valid_adjusters
),
latest AS (
    -- Keep the latest row per team_or_unknown
    SELECT
        id,
        level,
        level_filled,
        team_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team_or_unknown ORDER BY level DESC, id DESC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(level_filled) OVER (PARTITION BY level) AS level_min_level_filled,
        COUNT(*) OVER (PARTITION BY level) AS rows_in_level
    FROM latest
)
SELECT
    team_or_unknown,
    level_filled,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(rows_in_level) / NULLIF(SUM(level), 0), 3) AS rows_in_level_to_level_ratio,
    ROUND(STDDEV(level_min_level_filled), 2) AS stddev_level_min_level_filled
FROM sequenced
GROUP BY GROUPING SETS ((team_or_unknown, level_filled), (team_or_unknown), (level_filled))
