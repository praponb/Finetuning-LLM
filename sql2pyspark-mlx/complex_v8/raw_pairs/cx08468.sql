WITH recent_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT_WS('-', adjuster_name, team) AS adjuster_name_team_label,
        CASE WHEN level >= 3 THEN 'HIGH' WHEN level >= 1 THEN 'MEDIUM' ELSE 'LOW' END AS level_tier
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'G%'
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_name,
        level,
        adjuster_name_team_label,
        level_tier,
        LOWER(REGEXP_REPLACE(TRIM(level_tier), ' ', '_')) AS level_tier_slug
    FROM recent_adjusters
),
level_tier_stats AS (
    -- Max level per level_tier
    SELECT level_tier, MAX(level) AS max_level
    FROM labeled
    GROUP BY level_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_level, ROUND(s.level - g.max_level, 2) AS level_minus_max
    FROM labeled s
    JOIN level_tier_stats g ON g.level_tier = s.level_tier
    WHERE s.level = g.max_level
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_name,
        max_level,
        level_minus_max,
        COALESCE(level_minus_max, 0.0) AS level_minus_max_filled,
        max_level + level_minus_max AS max_level_plus_level_minus_max
    FROM compared
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, level_minus_max, level_minus_max_filled, max_level_plus_level_minus_max, max_level
    FROM tagged
) src
UNPIVOT (
    reading FOR field IN (level_minus_max, level_minus_max_filled, max_level_plus_level_minus_max, max_level)
) unp
ORDER BY id, field
