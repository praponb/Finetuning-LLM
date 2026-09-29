WITH valid_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(TRIM(adjuster_name)) AS adjuster_name_clean,
        SUBSTRING(TRIM(adjuster_name), 1, 4) AS adjuster_name_prefix
    FROM adjusters
    WHERE adjuster_name IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        team,
        adjuster_name_prefix,
        RANK() OVER (PARTITION BY team, level ORDER BY level ASC, id) AS level_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team, level ORDER BY level ASC, id), 3) AS level_percent_rank,
        ROUND(level / NULLIF(SUM(level) OVER w_level, 0), 4) AS level_share_of_level,
        ROUND(AVG(level) OVER (PARTITION BY level, team), 2) AS level_team_avg_level,
        NTH_VALUE(level, 2) OVER (PARTITION BY team, level ORDER BY level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        ROW_NUMBER() OVER w_all_by_level AS level_seq,
        SUM(level) OVER (PARTITION BY level, team) AS level_team_sum_level,
        COUNT(*) OVER (PARTITION BY level) AS rows_in_level
    FROM valid_adjusters
    WINDOW w_level AS (PARTITION BY level), w_all_by_level AS (ORDER BY level, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        adjuster_name,
        team,
        adjuster_name_prefix,
        level_percent_rank,
        level_share_of_level,
        level_team_avg_level,
        second_level,
        level_seq,
        level_team_sum_level,
        rows_in_level
    FROM metrics
    WHERE level_rank <= 5
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN rows_in_level >= 11 THEN 'large' WHEN rows_in_level >= 9 THEN 'medium' ELSE 'small' END AS rows_in_level_tier,
        COUNT(*) AS cnt,
        SUM(CASE WHEN adjuster_name IS NOT NULL THEN 1 ELSE 0 END) AS n_adjuster_name_is_not_null,
        MAX(level_share_of_level) AS max_level_share_of_level,
        ARRAY_SORT(COLLECT_SET(team)) AS team_list,
        COUNT(DISTINCT team) AS distinct_teams
    FROM top_rows
    GROUP BY CASE WHEN rows_in_level >= 11 THEN 'large' WHEN rows_in_level >= 9 THEN 'medium' ELSE 'small' END
    HAVING COUNT(*) >= 3
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        rows_in_level_tier,
        cnt,
        n_adjuster_name_is_not_null,
        max_level_share_of_level,
        distinct_teams,
        SIZE(COLLECT_SET(rows_in_level_tier) OVER (PARTITION BY n_adjuster_name_is_not_null)) AS distinct_rows_in_level_tier_in_n_adjuster_name_i,
        ROW_NUMBER() OVER (PARTITION BY rows_in_level_tier ORDER BY distinct_teams DESC, rows_in_level_tier) AS rn,
        NTH_VALUE(cnt, 2) OVER (PARTITION BY rows_in_level_tier ORDER BY distinct_teams DESC, rows_in_level_tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_cnt,
        MIN(distinct_teams) OVER w_n_adjuster_name_is_not_null AS n_adjuster_name_is_not_null_min_distinct_teams
    FROM per_group
    WINDOW w_n_adjuster_name_is_not_null AS (PARTITION BY n_adjuster_name_is_not_null)
)
SELECT
    rows_in_level_tier,
    cnt,
    n_adjuster_name_is_not_null,
    max_level_share_of_level,
    distinct_teams,
    distinct_rows_in_level_tier_in_n_adjuster_name_i,
    second_cnt,
    n_adjuster_name_is_not_null_min_distinct_teams,
    LOWER(REGEXP_REPLACE(TRIM(rows_in_level_tier), ' ', '_')) AS rows_in_level_tier_slug,
    COALESCE(rows_in_level_tier, 'UNKNOWN') AS rows_in_level_tier_or_unknown
FROM with_trends
ORDER BY n_adjuster_name_is_not_null NULLS LAST
