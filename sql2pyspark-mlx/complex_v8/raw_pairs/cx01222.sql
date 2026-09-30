WITH filtered_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CASE WHEN team IN ('Property', 'Health') THEN 'group_a' WHEN team = 'Travel' THEN 'group_b' ELSE 'other' END AS team_group,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
    FROM adjusters
    WHERE level > 3
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered_adjusters
    WHERE level > (SELECT AVG(level) FROM filtered_adjusters)
),
team_stats AS (
    -- Avg level per team
    SELECT team, AVG(level) AS avg_level
    FROM with_activity
    GROUP BY team
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_level, ROUND(s.level - g.avg_level, 2) AS level_minus_avg
    FROM with_activity s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level > g.avg_level
)
-- Final projection
SELECT
    id,
    team,
    level,
    team_group,
    avg_level,
    level_minus_avg,
    level_minus_avg - avg_level AS level_minus_avg_minus_avg_level
FROM compared
ORDER BY level DESC
