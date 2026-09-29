WITH prepared_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        UPPER(TRIM(team)) AS team_clean,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        UPPER(TRIM(team)) AS team_clean_2
    FROM adjusters
    WHERE level < 3
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(team_clean_2)) AS team_clean_2_clean,
        CONCAT_WS(': ', adjuster_name, team_clean) AS adjuster_name_team_clean_label,
        CONCAT_WS('-', team_clean, team_or_unknown) AS team_clean_team_or_unknown_label
    FROM prepared_adjusters
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.claim_type IN ('medical', 'flood')
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        team,
        level,
        team_clean,
        team_or_unknown,
        team_clean_2,
        team_clean_2_clean,
        adjuster_name_team_clean_label,
        team_clean_team_or_unknown_label,
        LOWER(REGEXP_REPLACE(TRIM(team_or_unknown), ' ', '_')) AS team_or_unknown_slug,
        CASE WHEN team_or_unknown IN ('Health', 'Motor') THEN 'group_a' WHEN team_or_unknown = 'UNKNOWN' THEN 'group_b' ELSE 'other' END AS team_or_unknown_group
    FROM without_matches
    WHERE team_or_unknown = 'Property'
),
annotated_2 AS (
    -- Derive helper columns
    SELECT
        id,
        team,
        level,
        team_or_unknown,
        team_clean_2,
        team_clean_2_clean,
        team_clean_team_or_unknown_label,
        team_or_unknown_slug,
        team_or_unknown_group,
        CASE WHEN level >= 3 THEN 'large' WHEN level >= 2 THEN 'medium' ELSE 'small' END AS level_tier
    FROM tagged
),
team_stats AS (
    -- Max level per team
    SELECT team, MAX(level) AS max_level
    FROM annotated_2
    GROUP BY team
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_level, ROUND(s.level - g.max_level, 2) AS level_minus_max
    FROM annotated_2 s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level = g.max_level
)
-- Final projection
SELECT
    id,
    team,
    level,
    team_or_unknown,
    team_clean_2,
    team_clean_team_or_unknown_label,
    team_or_unknown_slug,
    team_or_unknown_group,
    level_tier,
    max_level,
    level_minus_max,
    ROUND(level_minus_max * 1.1, 2) AS level_minus_max_adj
FROM outliers
ORDER BY team_or_unknown
