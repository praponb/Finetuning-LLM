WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 8
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.status IN ('REJECTED', 'OPEN', 'PAID')
              AND c.claim_date >= DATE '2024-04-15'
        )
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_team_level_by_level, 3) AS level_percent_rank,
        ROUND(AVG(level) OVER (PARTITION BY team, level ORDER BY level, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS level_avg_2_prior,
        LAST_VALUE(level) OVER (PARTITION BY team, level ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_level,
        FIRST_VALUE(level) OVER (PARTITION BY team, level ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level
    FROM filtered
    WINDOW w_team_level_by_level AS (PARTITION BY team, level ORDER BY level, id)
),
team_stats AS (
    -- Avg last_level per team
    SELECT team, AVG(last_level) AS avg_last_level
    FROM with_windows
    GROUP BY team
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_last_level,
        ROUND(s.last_level - g.avg_last_level, 2) AS last_level_minus_avg
    FROM with_windows s
    JOIN team_stats g ON g.team = s.team
    WHERE s.last_level > g.avg_last_level
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, last_level_minus_avg, level_avg_2_prior, level_percent_rank
    FROM vs_group
) src
UNPIVOT (
    value FOR metric_name IN (last_level_minus_avg, level_avg_2_prior, level_percent_rank)
) unp
