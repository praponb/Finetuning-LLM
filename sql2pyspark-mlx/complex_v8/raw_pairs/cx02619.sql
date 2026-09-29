WITH recent_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(level, 0) AS level_filled
    FROM adjusters
    WHERE team NOT IN ('Health', 'Motor')
      AND level >= 1
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM recent_adjusters s
    WHERE s.id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.claim_type = 'theft'
              AND c.policy_id <= 26
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (ORDER BY level ASC NULLS LAST) AS level_rank,
        NTH_VALUE(level_filled, 2) OVER (ORDER BY level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level_filled,
        MAX(level_filled) OVER (PARTITION BY level_filled) AS level_filled_max_level_filled,
        SUM(level_filled) OVER (PARTITION BY level_filled) AS level_filled_sum_level_filled
    FROM screened
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len,
        ROUND(level_filled / NULLIF(level, 0), 3) AS level_filled_per_level,
        SUBSTRING(TRIM(team), 1, 3) AS team_prefix
    FROM with_trends
    WHERE level_rank > 4
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(adjuster_name_len, 2) OVER (PARTITION BY level ORDER BY level_rank DESC, id) AS prev2_adjuster_name_len,
        LEAD(level_filled, 2, 0) OVER (ORDER BY level_filled_max_level_filled DESC, id) AS next2_level_filled
    FROM derived
)
-- Final projection
SELECT
    id,
    adjuster_name,
    team,
    second_level_filled,
    level_filled_max_level_filled,
    level_filled_sum_level_filled,
    team_prefix,
    prev2_adjuster_name_len,
    next2_level_filled,
    ABS(level_filled - 3) AS level_filled_distance,
    CASE WHEN prev2_adjuster_name_len >= 12 THEN 'large' WHEN prev2_adjuster_name_len >= 6 THEN 'medium' ELSE 'small' END AS prev2_adjuster_name_len_tier
FROM with_windows
ORDER BY team NULLS LAST, prev2_adjuster_name_len_tier DESC
