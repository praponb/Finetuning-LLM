WITH recent_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT_WS('-', adjuster_name, team) AS adjuster_name_team_label
    FROM adjusters
    WHERE adjuster_name IS NOT NULL
),
team_stats AS (
    -- Avg level per team
    SELECT team, AVG(level) AS avg_level
    FROM recent_adjusters
    GROUP BY team
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_level, ROUND(s.level - g.avg_level, 2) AS level_minus_avg
    FROM recent_adjusters s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level > g.avg_level
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY level, team ORDER BY level), 3) AS level_cume_dist,
        NTILE(10) OVER (PARTITION BY level, team ORDER BY level DESC, id) AS level_decile,
        NTH_VALUE(avg_level, 2) OVER (ORDER BY avg_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_avg_level,
        NTH_VALUE(level, 2) OVER (PARTITION BY level, team ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level
    FROM above_average
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.id) AS claim_count
    FROM with_trends s
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_name,
        team,
        adjuster_name_team_label,
        level_minus_avg,
        level_decile,
        second_avg_level,
        second_level,
        claim_count,
        SUM(avg_level) OVER (PARTITION BY level_decile, level ORDER BY level_minus_avg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_level,
        NTH_VALUE(second_avg_level, 2) OVER (PARTITION BY level_decile, level ORDER BY level_minus_avg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_avg_level,
        MIN(level_cume_dist) OVER (PARTITION BY claim_count, second_level) AS claim_count_second_level_min_level_cume_dist,
        LAG(second_avg_level) OVER w_level_decile_level_by_level_3 AS prev_second_avg_level,
        SUM(second_level) OVER (PARTITION BY claim_count, second_level) AS claim_count_second_level_sum_second_level
    FROM with_stats
    WINDOW w_level_decile_level_by_level_3 AS (PARTITION BY level_decile, level ORDER BY level_minus_avg DESC NULLS FIRST, id)
),
latest AS (
    -- Keep the earliest row per team
    SELECT
        id,
        adjuster_name,
        team,
        adjuster_name_team_label,
        level_minus_avg,
        level_decile,
        second_avg_level,
        second_level,
        claim_count,
        running_avg_level,
        second_second_avg_level,
        claim_count_second_level_min_level_cume_dist,
        prev_second_avg_level,
        claim_count_second_level_sum_second_level
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY level_minus_avg, id) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    adjuster_name,
    adjuster_name_team_label,
    level_minus_avg,
    level_decile,
    second_avg_level,
    claim_count,
    running_avg_level,
    second_second_avg_level,
    claim_count_second_level_min_level_cume_dist,
    prev_second_avg_level,
    claim_count_second_level_sum_second_level,
    SUBSTRING(TRIM(adjuster_name), 1, 2) AS adjuster_name_prefix
FROM latest
ORDER BY id
