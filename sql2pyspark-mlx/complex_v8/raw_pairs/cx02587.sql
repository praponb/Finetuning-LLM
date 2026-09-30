WITH filtered_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LOWER(adjuster_name) LIKE '%ar%'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(level) OVER (PARTITION BY team, level ORDER BY level DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS level_sum_1_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY level, team ORDER BY level DESC, id), 3) AS level_cume_dist
    FROM filtered_adjusters
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(level_cume_dist - AVG(level_cume_dist) OVER (PARTITION BY level, team), 2) AS level_cume_dist_vs_level_team_avg,
        ROUND(level_sum_1_prior - AVG(level_sum_1_prior) OVER (PARTITION BY level, team), 2) AS level_sum_1_prior_vs_level_team_avg,
        ROUND(level - AVG(level) OVER (PARTITION BY level, team), 2) AS level_vs_level_team_avg,
        ROUND(level_sum_1_prior / NULLIF(SUM(level_sum_1_prior) OVER (PARTITION BY level, team), 0), 4) AS level_sum_1_prior_share_of_level_team
    FROM windowed
),
team_stats AS (
    -- Max level per team
    SELECT team, MAX(level) AS max_level
    FROM with_trends
    GROUP BY team
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_level, ROUND(s.level - g.max_level, 2) AS level_minus_max
    FROM with_trends s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level = g.max_level
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, SUM(approved_amount) AS total_claim_approved_amount
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount
    FROM compared s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
)
SELECT
    id,
    adjuster_name,
    team,
    level,
    level_sum_1_prior,
    level_cume_dist,
    level_sum_1_prior_vs_level_team_avg,
    level_minus_max,
    total_claim_approved_amount,
    COALESCE(team, 'UNKNOWN') AS team_or_unknown
FROM profiled
ORDER BY level_sum_1_prior_vs_level_team_avg, id DESC
