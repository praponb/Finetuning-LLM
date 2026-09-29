WITH scoped_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CASE WHEN team IN ('Property', 'Travel') THEN 'group_a' WHEN team = 'Health' THEN 'group_b' ELSE 'other' END AS team_group,
        CONCAT(adjuster_name, '#', CAST(id AS STRING)) AS adjuster_name_ref
    FROM adjusters
    WHERE level < 3
      AND LENGTH(TRIM(adjuster_name)) >= 4
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MIN(reported_at) AS first_claim_reported_at
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at
    FROM scoped_adjusters s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
team_group_stats AS (
    -- Avg level per team_group
    SELECT team_group, AVG(level) AS avg_level
    FROM with_stats
    GROUP BY team_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_level, ROUND(s.level - g.avg_level, 2) AS level_minus_avg
    FROM with_stats s
    JOIN team_group_stats g ON g.team_group = s.team_group
    WHERE s.level > g.avg_level
),
grouped AS (
    -- Group-level totals
    SELECT
        team,
        team_group,
        level,
        COUNT(*) FILTER (WHERE team IS NOT NULL) AS n_team_is_not_null,
        COUNT(*) AS row_count,
        MAX(first_claim_reported_at) AS last_first_claim_reported_at
    FROM compared
    WHERE team_group = 'group_a'
    GROUP BY 1, 2, 3
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE row_count > (SELECT AVG(row_count) FROM grouped)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE level > (SELECT AVG(level) FROM filtered)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', team_group, team) AS team_group_team_label,
        SUBSTRING(TRIM(team_group), 1, 4) AS team_group_prefix
    FROM with_activity
    WHERE team_group <> 'group_b'
)
SELECT
    team,
    level,
    n_team_is_not_null,
    row_count,
    last_first_claim_reported_at,
    team_group_team_label,
    team_group_prefix,
    CASE WHEN team_group IN ('group_a', 'other') THEN 'group_a' WHEN team_group = 'group_b' THEN 'group_b' ELSE 'other' END AS team_group_group,
    LEAST(row_count, n_team_is_not_null) AS least_row_count_n_team_is_not_null
FROM labeled
ORDER BY row_count DESC NULLS FIRST
