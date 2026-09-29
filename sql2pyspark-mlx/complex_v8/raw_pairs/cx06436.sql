WITH scoped AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        ABS(level - 3) AS level_distance,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
    FROM adjusters
    WHERE level > 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(level_distance, 2) OVER (ORDER BY level_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level_distance,
        SUM(level_distance) OVER w_level_adjuster_name_len AS level_adjuster_name_len_sum_level_distance,
        ROUND(level_distance / NULLIF(SUM(level_distance) OVER (PARTITION BY level, adjuster_name_len), 0), 4) AS level_distance_share_of_level_adjuster_name_len,
        COUNT(*) OVER (PARTITION BY level, adjuster_name_len) AS rows_in_level_adjuster_name_len
    FROM scoped
    WINDOW w_level_adjuster_name_len AS (PARTITION BY level, adjuster_name_len)
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.policy_id IS NOT NULL
        )
      AND LENGTH(TRIM(s.adjuster_name)) >= 5
)
-- Final projection
SELECT
    id,
    adjuster_name,
    team_or_unknown,
    level_distance,
    adjuster_name_len,
    level_adjuster_name_len_sum_level_distance,
    level_distance_share_of_level_adjuster_name_len
FROM without_matches
WHERE level_distance >= 1
ORDER BY level_distance DESC NULLS FIRST, level_distance_share_of_level_adjuster_name_len NULLS LAST
