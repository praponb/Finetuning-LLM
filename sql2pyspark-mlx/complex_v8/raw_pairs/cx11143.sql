WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(adjuster_name), 1, 4) AS adjuster_name_prefix
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 6
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS first_claim_reported_at
    FROM cleaned s
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        level,
        adjuster_name_prefix,
        first_claim_reported_at,
        level - LAG(level) OVER (PARTITION BY team ORDER BY level DESC, id) AS level_change,
        ROUND(PERCENT_RANK() OVER w_team_by_level_ties, 3) AS level_percent_rank
    FROM profiled
    WINDOW w_team_by_level_ties AS (PARTITION BY team ORDER BY level DESC)
)
-- Subtotals with GROUPING SETS
SELECT
    level_change,
    level,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(level), 2) AS stddev_level,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(first_claim_reported_at) AS first_first_claim_reported_at
FROM ranked
GROUP BY GROUPING SETS ((level_change, level), (level_change), ())
