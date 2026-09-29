WITH scoped AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE level < 3
      AND team = 'Travel'
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.id) AS last_claim_date,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS first_claim_reported_at
    FROM scoped s
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.id) AS min_claim_claimed_amount,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.id) AS last_claim_date_2
    FROM profiled s
),
team_stats AS (
    -- Median level per team
    SELECT team, PERCENTILE_APPROX(level, 0.5) AS median_level
    FROM with_stats
    GROUP BY team
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_level,
        ROUND(s.level - g.median_level, 2) AS level_minus_median
    FROM with_stats s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level >= g.median_level
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        team,
        DATE_TRUNC('MONTH', last_claim_date) AS last_claim_date_month,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        MAX(level_minus_median) AS max_level_minus_median,
        COUNT(*) AS row_count
    FROM vs_group
    WHERE last_claim_date >= DATE '2024-08-01'
    GROUP BY team, DATE_TRUNC('MONTH', last_claim_date)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(last_claim_date_month), team ORDER BY max_level_minus_median DESC), 3) AS max_level_minus_median_percent_rank,
        LAST_VALUE(last_claim_date_month) OVER (PARTITION BY YEAR(last_claim_date_month), team ORDER BY max_level_minus_median DESC, team, last_claim_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_claim_date_month
    FROM summary
)
-- Final projection
SELECT
    team,
    adjuster_name_list,
    max_level_minus_median,
    row_count,
    last_last_claim_date_month,
    LEAST(max_level_minus_median, row_count) AS least_max_level_minus_median_row_count
FROM with_trends
WHERE team NOT IN ('Property', 'Travel')
ORDER BY least_max_level_minus_median_row_count DESC, max_level_minus_median DESC NULLS FIRST
