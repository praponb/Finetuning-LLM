WITH cleaned AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
    FROM adjusters
),
team_stats AS (
    -- Avg level per team
    SELECT team, AVG(level) AS avg_level
    FROM cleaned
    GROUP BY team
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_level, ROUND(s.level - g.avg_level, 2) AS level_minus_avg
    FROM cleaned s
    JOIN team_stats g ON g.team = s.team
    WHERE s.level > g.avg_level
),
latest_per_key AS (
    -- Keep the latest row per team
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_len,
        avg_level,
        level_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY avg_level DESC, id DESC) AS rn
        FROM compared
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LAST_VALUE(avg_level) OVER (PARTITION BY team ORDER BY adjuster_name_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_avg_level,
        LAST_VALUE(avg_level) IGNORE NULLS OVER (PARTITION BY adjuster_name_len ORDER BY level_minus_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_avg_level,
        LAG(level_minus_avg, 2) OVER (PARTITION BY team ORDER BY adjuster_name_len ASC, id) AS prev2_level_minus_avg,
        DENSE_RANK() OVER (PARTITION BY team ORDER BY adjuster_name_len ASC, id) AS adjuster_name_len_dense_rank,
        ROUND(AVG(avg_level) OVER (PARTITION BY adjuster_name_len ORDER BY level_minus_avg, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS avg_level_avg_6_prior,
        MIN(adjuster_name_len) OVER (PARTITION BY adjuster_name_len ORDER BY level_minus_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_adjuster_name_len_so_far
    FROM latest_per_key
),
grouped AS (
    -- Group-level totals
    SELECT
        CASE WHEN prev2_level_minus_avg >= 1.5 THEN 'A' WHEN prev2_level_minus_avg >= -1.0 THEN 'B' ELSE 'C' END AS prev2_level_minus_avg_tier,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        ROUND(SUM(last_known_avg_level) / NULLIF(SUM(prev2_level_minus_avg), 0), 3) AS last_known_avg_level_to_prev2_level_minus_avg_ra,
        ROUND(STDDEV(level), 2) AS stddev_level,
        MIN(prev2_level_minus_avg) AS min_prev2_level_minus_avg,
        ROUND(SUM(last_known_avg_level) / NULLIF(SUM(last_avg_level), 0), 3) AS last_known_avg_level_to_last_avg_level_ratio
    FROM metrics
    GROUP BY CASE WHEN prev2_level_minus_avg >= 1.5 THEN 'A' WHEN prev2_level_minus_avg >= -1.0 THEN 'B' ELSE 'C' END
    HAVING MAX(last_avg_level) > 1.6
),
tagged AS (
    -- Label and score rows
    SELECT
        prev2_level_minus_avg_tier,
        adjuster_name_list,
        last_known_avg_level_to_prev2_level_minus_avg_ra,
        stddev_level,
        min_prev2_level_minus_avg,
        last_known_avg_level_to_last_avg_level_ratio,
        ROUND(stddev_level * 1.5, 2) AS stddev_level_adj,
        ROUND(stddev_level * 1.5, 2) AS stddev_level_adj_2,
        ABS(stddev_level - 2.8) AS stddev_level_distance
    FROM grouped
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(stddev_level / NULLIF(stddev_level_adj_2, 0), 3) AS stddev_level_per_stddev_level_adj_2,
        ARRAY_JOIN(ARRAY_SORT(adjuster_name_list), ',') AS adjuster_name_list_text,
        COALESCE(stddev_level, 0.0) AS stddev_level_filled
    FROM tagged
    WHERE stddev_level_distance <= 3.5
)
SELECT
    prev2_level_minus_avg_tier,
    adjuster_name_list,
    last_known_avg_level_to_prev2_level_minus_avg_ra,
    stddev_level,
    last_known_avg_level_to_last_avg_level_ratio,
    stddev_level_adj_2,
    stddev_level_distance,
    stddev_level_per_stddev_level_adj_2,
    adjuster_name_list_text,
    stddev_level_filled
FROM labeled
ORDER BY stddev_level_filled
