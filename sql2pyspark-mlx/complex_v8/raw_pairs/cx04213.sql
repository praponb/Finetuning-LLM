WITH filtered_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(team), 1, 3) AS team_prefix
    FROM adjusters
    WHERE team <> 'Travel'
),
enriched AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(team_prefix), 1, 4) AS team_prefix_prefix
    FROM filtered_adjusters
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        level,
        team_prefix,
        team_prefix_prefix,
        ROUND(AVG(level) OVER (PARTITION BY team ORDER BY level DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS level_avg_6_prior,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY team ORDER BY level DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        NTH_VALUE(level, 2) OVER (PARTITION BY team ORDER BY level DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        COUNT(*) OVER (PARTITION BY team, level) AS rows_in_team_level
    FROM enriched
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MAX(claimed_amount) AS max_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.max_claim_claimed_amount
    FROM metrics s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
)
SELECT
    second_level,
    first_known_level,
    level,
    MIN(level) AS min_level
FROM profiled
GROUP BY GROUPING SETS ((second_level, first_known_level, level), (second_level), ())
ORDER BY second_level NULLS LAST, first_known_level NULLS LAST, level NULLS LAST
