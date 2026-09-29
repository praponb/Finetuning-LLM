WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled,
        SUBSTRING(TRIM(level), 1, 4) AS level_prefix
    FROM courses
),
dept_stats AS (
    -- Median credits per dept
    SELECT dept, PERCENTILE_APPROX(credits, 0.5) AS median_credits
    FROM base
    GROUP BY dept
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_credits,
        ROUND(s.credits - g.median_credits, 2) AS credits_minus_median
    FROM base s
    JOIN dept_stats g ON g.dept = s.dept
    WHERE s.credits >= g.median_credits
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        credits_filled,
        dept,
        MAX(credits_filled) AS max_credits_filled,
        MIN(credits_minus_median) AS min_credits_minus_median,
        SUM(credits) AS total_credits,
        ARRAY_SORT(COLLECT_SET(level)) AS level_list,
        COUNT(DISTINCT dept) AS distinct_depts,
        MIN(credits) AS min_credits
    FROM compared
    WHERE credits_minus_median > -2.4
    GROUP BY 1, 2
    HAVING AVG(credits_filled) > 2
)
-- Final projection
SELECT
    credits_filled,
    dept,
    min_credits_minus_median,
    total_credits,
    level_list,
    min_credits,
    ABS(credits_filled - 3) AS credits_filled_distance,
    ROUND(credits_filled / NULLIF(total_credits, 0), 3) AS credits_filled_per_total_credits
FROM summary
ORDER BY min_credits_minus_median DESC, credits_filled, dept
LIMIT 20
