WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE level <= 3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(level) OVER (PARTITION BY level, team ORDER BY level DESC, id) AS next_level,
        ROUND(PERCENT_RANK() OVER (PARTITION BY level, team ORDER BY level DESC), 3) AS level_percent_rank,
        ROUND(level - AVG(level) OVER (PARTITION BY level), 2) AS level_vs_level_avg
    FROM valid
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) AS claim_count,
        MIN(claimed_amount) AS min_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.min_claim_claimed_amount
    FROM windowed s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_name,
        team,
        next_level,
        level_percent_rank,
        level_vs_level_avg,
        claim_count,
        min_claim_claimed_amount,
        SIZE(COLLECT_SET(team) OVER (PARTITION BY level)) AS distinct_team_in_level,
        COUNT(*) OVER (PARTITION BY level) AS rows_in_level,
        SUM(min_claim_claimed_amount) OVER (PARTITION BY team, level ORDER BY level_vs_level_avg DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS min_claim_claimed_amount_sum_6_prior,
        SUM(next_level) OVER (PARTITION BY next_level ORDER BY level_vs_level_avg, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS next_level_sum_1_prior,
        MAX(min_claim_claimed_amount) OVER (PARTITION BY level) AS level_max_min_claim_claimed_amount,
        ROUND(AVG(min_claim_claimed_amount) OVER (PARTITION BY level), 2) AS level_avg_min_claim_claimed_amount
    FROM profiled
)
-- Final projection
SELECT
    id,
    adjuster_name,
    level_percent_rank,
    level_vs_level_avg,
    claim_count,
    min_claim_claimed_amount,
    rows_in_level,
    min_claim_claimed_amount_sum_6_prior,
    level_max_min_claim_claimed_amount,
    level_avg_min_claim_claimed_amount,
    next_level_sum_1_prior - level_vs_level_avg AS next_level_sum_1_prior_minus_level_vs_level_avg
FROM sequenced
ORDER BY level_vs_level_avg DESC
