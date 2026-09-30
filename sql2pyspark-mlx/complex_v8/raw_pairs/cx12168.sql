WITH valid AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT(adjuster_name, '#', CAST(level AS STRING)) AS adjuster_name_ref,
        CASE WHEN level >= 3 THEN 'HIGH' WHEN level >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS level_tier,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'F%'
      AND level <= 3
),
rollup_base AS (
    -- Summarise per group
    SELECT
        level,
        level_tier,
        COUNT(DISTINCT level_tier) AS distinct_level_tiers,
        SUM(adjuster_name_len) AS total_adjuster_name_len,
        COUNT(*) AS cnt,
        COUNT(DISTINCT team) AS distinct_teams
    FROM valid
    GROUP BY level, level_tier
    HAVING AVG(adjuster_name_len) > 5
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        level,
        level_tier,
        cnt,
        distinct_teams,
        DENSE_RANK() OVER (PARTITION BY cnt ORDER BY distinct_level_tiers, level, level_tier) AS distinct_level_tiers_dense_rank,
        ROUND(distinct_level_tiers - AVG(distinct_level_tiers) OVER (PARTITION BY level, distinct_level_tiers), 2) AS distinct_level_tiers_vs_level_distinct_level_tie,
        FIRST_VALUE(distinct_teams) OVER (PARTITION BY cnt ORDER BY distinct_level_tiers, level, level_tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_teams
    FROM rollup_base
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE distinct_level_tiers_dense_rank <= 2
      AND cnt <= 10
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT cnt, level, level_tier, distinct_level_tiers_dense_rank
    FROM leaders
) src
PIVOT (
    COUNT(distinct_level_tiers_dense_rank)
    FOR level_tier IN ('LOW', 'MEDIUM')
)
