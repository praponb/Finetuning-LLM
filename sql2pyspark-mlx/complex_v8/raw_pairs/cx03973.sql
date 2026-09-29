WITH base_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CASE WHEN team IN ('Property', 'Health') THEN 'group_a' WHEN team = 'Motor' THEN 'group_b' ELSE 'other' END AS team_group
    FROM adjusters
    WHERE team = 'Motor'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(team_group) OVER (PARTITION BY team)) AS distinct_team_group_in_team,
        ROUND(AVG(level) OVER (PARTITION BY team_group), 2) AS team_group_avg_level,
        LEAD(level, 1, 0) OVER (PARTITION BY level, team_group ORDER BY level DESC, id) AS next_level,
        SUM(level) OVER (PARTITION BY level, team_group ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_level,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY team), 0), 4) AS level_share_of_team,
        COUNT(*) OVER (PARTITION BY team) AS rows_in_team,
        SIZE(COLLECT_SET(team) OVER w_team_group) AS distinct_team_in_team_group,
        ROW_NUMBER() OVER (PARTITION BY level, team ORDER BY level ASC NULLS LAST, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY team ORDER BY level DESC, id) AS level_dense_rank,
        ROUND(AVG(level) OVER (PARTITION BY team ORDER BY level DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS level_avg_7,
        level - LAG(level) OVER (PARTITION BY team ORDER BY level DESC, id) AS level_change,
        SUM(level) OVER (PARTITION BY team ORDER BY level DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS level_sum_2,
        NTILE(4) OVER (PARTITION BY team ORDER BY level DESC, id) AS level_quartile
    FROM base_adjusters
    WINDOW w_team_group AS (PARTITION BY team_group)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN rows_in_team >= 12 THEN 'A' WHEN rows_in_team >= 7 THEN 'B' ELSE 'C' END AS rows_in_team_tier
    FROM windowed
    WHERE adjuster_name IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(next_level / NULLIF(SUM(next_level) OVER (PARTITION BY team_group), 0), 4) AS next_level_share_of_team_group,
        SUM(level_change) OVER (PARTITION BY team_group) AS team_group_sum_level_change,
        SUM(distinct_team_group_in_team) OVER (ORDER BY rows_in_team DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS distinct_team_group_in_team_sum_4,
        COUNT(*) OVER (PARTITION BY team_group) AS rows_in_team_group
    FROM annotated
)
-- Final projection
SELECT
    id,
    adjuster_name,
    team,
    level,
    distinct_team_group_in_team,
    team_group_avg_level,
    next_level,
    running_level,
    rows_in_team,
    distinct_team_in_team_group,
    level_dense_rank,
    level_sum_2,
    rows_in_team_tier,
    next_level_share_of_team_group,
    team_group_sum_level_change,
    rows_in_team_group,
    ROUND(level_change / NULLIF(level_dense_rank, 0), 3) AS level_change_per_level_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(team_group), ' ', '_')) AS team_group_slug
FROM with_windows
WHERE team_group_avg_level >= 1.6
ORDER BY level_dense_rank DESC
