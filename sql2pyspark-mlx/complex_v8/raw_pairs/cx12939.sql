WITH filtered_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(adjuster_name), 1, 4) AS adjuster_name_prefix
    FROM adjusters
    WHERE level BETWEEN 1 AND 3
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        team,
        level,
        adjuster_name_prefix,
        ROUND(AVG(level) OVER (PARTITION BY team, level ORDER BY level ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS level_avg_2_prior,
        FIRST_VALUE(level) OVER (PARTITION BY team, level ORDER BY level ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level,
        ROUND(level - AVG(level) OVER (PARTITION BY team, level), 2) AS level_vs_team_level_avg,
        ROUND(PERCENT_RANK() OVER w_team_level_by_level, 3) AS level_percent_rank
    FROM filtered_adjusters
    WINDOW w_team_level_by_level AS (PARTITION BY team, level ORDER BY level ASC NULLS LAST, id)
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
        )
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.team,
        s.adjuster_name_prefix,
        s.level_avg_2_prior,
        s.first_level,
        s.level_vs_team_level_avg,
        c.id AS claim_id,
        c.claim_date,
        c.policy_id AS claim_policy_id,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at
    FROM filtered s
    JOIN claims c ON c.adjuster_id = s.id
)
SELECT id, claim_id, metric_name, amount
FROM (
    SELECT id, claim_id, level_avg_2_prior, claim_claimed_amount, level_vs_team_level_avg
    FROM with_children
) src
UNPIVOT (
    amount FOR metric_name IN (level_avg_2_prior, claim_claimed_amount, level_vs_team_level_avg)
) unp
