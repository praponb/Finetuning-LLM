WITH valid_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN level IN ('advanced', 'core') THEN 'group_a' WHEN level = 'intro' THEN 'group_b' ELSE 'other' END AS level_group
    FROM courses
    WHERE dept IS NOT NULL
),
stats AS (
    -- Summarise per group
    SELECT
        level_group,
        SUM(credits) AS total_credits,
        MAX(credits) AS max_credits,
        PERCENTILE_APPROX(credits, 0.5) AS median_credits,
        ROUND(100 * COUNT(CASE WHEN level = 'advanced' THEN 1 END) / COUNT(*), 1) AS pct_level_advanced
    FROM valid_courses
    GROUP BY level_group
    HAVING SUM(credits) > 2
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE pct_level_advanced > (SELECT AVG(pct_level_advanced) FROM stats)
),
scored AS (
    -- Add calculated columns
    SELECT
        level_group,
        median_credits,
        pct_level_advanced,
        LENGTH(TRIM(level_group)) AS level_group_len,
        CASE WHEN level_group IN ('group_a', 'other') THEN 'group_a' WHEN level_group = 'group_b' THEN 'group_b' ELSE 'other' END AS level_group_group
    FROM filtered
    WHERE level_group = 'group_a'
),
level_group_group_stats AS (
    -- Max pct_level_advanced per level_group_group
    SELECT level_group_group, MAX(pct_level_advanced) AS max_pct_level_advanced
    FROM scored
    GROUP BY level_group_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_pct_level_advanced,
        ROUND(s.pct_level_advanced - g.max_pct_level_advanced, 2) AS pct_level_advanced_minus_max
    FROM scored s
    JOIN level_group_group_stats g ON g.level_group_group = s.level_group_group
    WHERE s.pct_level_advanced = g.max_pct_level_advanced
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        level_group,
        median_credits,
        pct_level_advanced,
        level_group_group,
        RANK() OVER (PARTITION BY level_group_len, level_group ORDER BY median_credits, level_group) AS median_credits_rank,
        FIRST_VALUE(pct_level_advanced) IGNORE NULLS OVER (PARTITION BY level_group_len, level_group ORDER BY median_credits, level_group ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pct_level_advanced,
        DENSE_RANK() OVER (PARTITION BY level_group_len, level_group ORDER BY median_credits) AS median_credits_dense_rank,
        LEAD(level_group_len) OVER w_level_group_len_level_group AS next_level_group_len,
        ROUND(CUME_DIST() OVER (PARTITION BY level_group_len, level_group ORDER BY median_credits), 3) AS median_credits_cume_dist
    FROM vs_group
    WINDOW w_level_group_len_level_group AS (PARTITION BY level_group_len, level_group ORDER BY median_credits, level_group)
),
enriched AS (
    -- Add calculated columns
    SELECT
        level_group,
        median_credits,
        pct_level_advanced,
        level_group_group,
        median_credits_rank,
        median_credits_dense_rank,
        next_level_group_len,
        median_credits_cume_dist,
        pct_level_advanced - median_credits_rank AS pct_level_advanced_minus_median_credits_rank
    FROM metrics
)
-- Native SQL UNPIVOT (wide to long)
SELECT level_group, field, reading
FROM (
    SELECT level_group, median_credits, next_level_group_len, median_credits_rank, median_credits_dense_rank
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (median_credits, next_level_group_len, median_credits_rank, median_credits_dense_rank)
) unp
