WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS last_claim_reported_at,
        (SELECT ROUND(AVG(c.claimed_amount), 2) FROM claims c WHERE c.adjuster_id = s.id) AS avg_claim_claimed_amount,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS first_claim_reported_at
    FROM base s
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        level,
        YEAR(first_claim_reported_at) AS first_claim_reported_at_year,
        DATE_TRUNC('MONTH', last_claim_reported_at) AS last_claim_reported_at_month,
        COUNT(*) FILTER (WHERE team = 'Property') AS n_team_property,
        ROUND(100 * COUNT(CASE WHEN team NOT IN ('Property', 'Health') THEN 1 END) / COUNT(*), 1) AS pct_team_not_in_property_health
    FROM profiled
    GROUP BY level, YEAR(first_claim_reported_at), DATE_TRUNC('MONTH', last_claim_reported_at)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY pct_team_not_in_property_health DESC, level, first_claim_reported_at_year, last_claim_reported_at_month) AS seq_no,
        COUNT(*) OVER w_level_first_claim_reported_a AS rows_in_level_first_claim_reported_at_year,
        LAST_VALUE(level) OVER (ORDER BY pct_team_not_in_property_health DESC, level, first_claim_reported_at_year, last_claim_reported_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_level,
        LAG(first_claim_reported_at_year) OVER (PARTITION BY level, DATE_TRUNC('MONTH', last_claim_reported_at_month) ORDER BY n_team_property, level, first_claim_reported_at_year, last_claim_reported_at_month) AS prev_first_claim_reported_at_year
    FROM per_group
    WINDOW w_level_first_claim_reported_a AS (PARTITION BY level, first_claim_reported_at_year)
),
tagged AS (
    -- Add calculated columns
    SELECT *, COALESCE(level, 0) AS level_filled
    FROM sequenced
    WHERE pct_team_not_in_property_health <= 55.0
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN level_filled >= 4 THEN 'large' WHEN level_filled >= 2 THEN 'medium' ELSE 'small' END AS level_filled_tier,
        level_filled + n_team_property AS level_filled_plus_n_team_property,
        ABS(seq_no - 4) AS seq_no_distance
    FROM tagged
)
-- Subtotals with GROUPING SETS
SELECT
    prev_first_claim_reported_at_year,
    level_filled,
    first_claim_reported_at_year,
    SUM(level) AS total_level,
    MIN(rows_in_level_first_claim_reported_at_year) AS min_rows_in_level_first_claim_reported_at_year
FROM annotated
GROUP BY GROUPING SETS ((prev_first_claim_reported_at_year, level_filled, first_claim_reported_at_year), (prev_first_claim_reported_at_year), (level_filled), ())
ORDER BY prev_first_claim_reported_at_year NULLS LAST, level_filled NULLS LAST, first_claim_reported_at_year NULLS LAST
