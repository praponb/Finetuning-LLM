WITH prepared_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT(adjuster_name, '#', CAST(level AS STRING)) AS adjuster_name_ref,
        INITCAP(TRIM(adjuster_name)) AS adjuster_name_clean,
        SUBSTRING(TRIM(team), 1, 4) AS team_prefix
    FROM adjusters
    WHERE adjuster_name IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_level_team_by_level_ties, 3) AS level_percent_rank,
        SUM(level) OVER (PARTITION BY level, team ORDER BY level, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS level_sum_1_prior,
        LAG(level) OVER (PARTITION BY level, team ORDER BY level, id) AS prev_level,
        LAST_VALUE(level) IGNORE NULLS OVER (ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level
    FROM prepared_adjusters
    WINDOW w_level_team_by_level_ties AS (PARTITION BY level, team ORDER BY level), w_level_team_by_level AS (PARTITION BY level, team ORDER BY level, id)
)
-- Subtotals with GROUPING SETS
SELECT
    last_known_level,
    COALESCE(team, 'ALL') AS team,
    GROUPING_ID() AS grouping_level,
    MAX(level) AS max_level,
    ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(adjuster_name)) LIKE 'G%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_adjuster_name_lik
FROM with_windows
GROUP BY GROUPING SETS ((last_known_level, team), (last_known_level), (team), ())
