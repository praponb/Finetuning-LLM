WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LOWER(adjuster_name) LIKE '% g%'
      AND level < 3
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(level - 3) AS level_distance,
        SUBSTRING(TRIM(adjuster_name), 1, 2) AS adjuster_name_prefix,
        CASE WHEN team IN ('Travel', 'Health') THEN 'group_a' WHEN team = 'Property' THEN 'group_b' ELSE 'other' END AS team_group
    FROM filtered
    WHERE level >= 1
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) FILTER (WHERE claim_type = 'fire') AS claim_claim_type_count
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM labeled s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(team_group) OVER (PARTITION BY team, level)) AS distinct_team_group_in_team_level,
        SUM(level) OVER (PARTITION BY team, level_distance ORDER BY level DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS level_sum_3
    FROM profiled
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level, claim_claim_type_count ORDER BY claim_claim_type_count) AS claim_claim_type_count_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_team_group_in_team_level ORDER BY distinct_team_group_in_team_level NULLS LAST, id), 3) AS distinct_team_group_in_team_level_cume_dist,
        SUM(claim_claim_type_count) OVER (PARTITION BY level, claim_claim_type_count ORDER BY claim_claim_type_count, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claim_claim_type_count_sum_7,
        ROUND(level_distance / NULLIF(SUM(level_distance) OVER (PARTITION BY team_group), 0), 4) AS level_distance_share_of_team_group,
        NTH_VALUE(claim_claim_type_count, 2) OVER (PARTITION BY level, claim_claim_type_count ORDER BY claim_claim_type_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_claim_type_count
    FROM sequenced
),
per_group AS (
    -- Summarise per group
    SELECT
        level,
        team,
        claim_claim_type_count_dense_rank,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        COUNT(*) FILTER (WHERE adjuster_name IS NOT NULL) AS n_adjuster_name_is_not_null
    FROM ranked
    GROUP BY 1, 2, 3
    HAVING AVG(second_claim_claim_type_count) > 2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(level, 2) OVER (PARTITION BY level ORDER BY n_adjuster_name_is_not_null DESC, level, team, claim_claim_type_count_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        ROUND(level - AVG(level) OVER (PARTITION BY level, team), 2) AS level_vs_level_team_avg,
        NTH_VALUE(level, 2) OVER (PARTITION BY claim_claim_type_count_dense_rank ORDER BY n_adjuster_name_is_not_null DESC, level, team, claim_claim_type_count_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level_2,
        MIN(level) OVER (PARTITION BY level, team) AS level_team_min_level,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY level ORDER BY n_adjuster_name_is_not_null DESC, level, team, claim_claim_type_count_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY level ORDER BY n_adjuster_name_is_not_null DESC, level, team, claim_claim_type_count_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level
    FROM per_group
)
-- Subtotals with GROUPING SETS
SELECT
    first_known_level,
    second_level_2,
    GROUPING(first_known_level) AS is_total_first_known_level,
    ARRAY_SORT(COLLECT_SET(team)) AS team_list
FROM metrics
GROUP BY GROUPING SETS ((first_known_level, second_level_2), (first_known_level))
ORDER BY first_known_level NULLS LAST, second_level_2 NULLS LAST
