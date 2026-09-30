WITH recent_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug,
        SUBSTRING(TRIM(adjuster_name), 1, 3) AS adjuster_name_prefix,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM adjusters
    WHERE LOWER(adjuster_name) LIKE '%su%'
      AND level < 3
),
team_or_unknown_stats AS (
    -- Max level per team_or_unknown
    SELECT team_or_unknown, MAX(level) AS max_level
    FROM recent_adjusters
    GROUP BY team_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_level, ROUND(s.level - g.max_level, 2) AS level_minus_max
    FROM recent_adjusters s
    JOIN team_or_unknown_stats g ON g.team_or_unknown = s.team_or_unknown
    WHERE s.level = g.max_level
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        team,
        level,
        adjuster_name_prefix,
        max_level,
        level_minus_max,
        INITCAP(TRIM(team_slug)) AS team_slug_clean,
        ABS(level_minus_max - 0.5) AS level_minus_max_distance,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug_2
    FROM outliers
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.reported_at >= TIMESTAMP '2025-01-25 14:00:00'
              AND c.claim_type = 'medical'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, max_level, level_minus_max
    FROM screened
) src
UNPIVOT (
    reading FOR metric_name IN (max_level AS max_level_v, level_minus_max AS m_level_minus_max)
) unp
