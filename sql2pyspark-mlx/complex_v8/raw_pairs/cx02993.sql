WITH recent_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LENGTH(TRIM(team)) AS team_len,
        COALESCE(level, 0) AS level_filled,
        CASE WHEN team IN ('Motor', 'Health') THEN 'group_a' WHEN team = 'Property' THEN 'group_b' ELSE 'other' END AS team_group
    FROM adjusters
    WHERE team NOT IN ('Property', 'Motor')
      AND UPPER(TRIM(adjuster_name)) LIKE 'H%'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_len,
        level_filled,
        ROW_NUMBER() OVER w_team_level_filled_by_level AS rn,
        SIZE(COLLECT_SET(team) OVER w_level_filled) AS distinct_team_in_level_filled,
        DENSE_RANK() OVER w_level_filled_by_team_len_tie AS team_len_dense_rank,
        DENSE_RANK() OVER (PARTITION BY team, level_filled ORDER BY level) AS level_dense_rank,
        COUNT(*) OVER w_level_filled AS rows_in_level_filled,
        ROUND(team_len - AVG(team_len) OVER (PARTITION BY level_filled), 2) AS team_len_vs_level_filled_avg
    FROM recent_adjusters
    WINDOW w_team_level_filled_by_level AS (PARTITION BY team, level_filled ORDER BY level, id), w_level_filled AS (PARTITION BY level_filled), w_level_filled_by_team_len_tie AS (PARTITION BY level_filled ORDER BY team_len)
)
-- Subtotals with CUBE
SELECT
    rn,
    team_len,
    team_len_dense_rank,
    COUNT(*) FILTER (WHERE team IN ('Motor', 'Property')) AS n_team_in_motor_property,
    COUNT(*) AS n_rows
FROM windowed
GROUP BY CUBE(rn, team_len, team_len_dense_rank)
ORDER BY rn NULLS LAST, team_len NULLS LAST, team_len_dense_rank NULLS LAST
