WITH scoped AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'C%'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(credits) OVER (PARTITION BY dept ORDER BY credits, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS credits_avg_2,
        LAG(credits, 3, 0) OVER w_dept_by_credits AS prev3_credits,
        DENSE_RANK() OVER (PARTITION BY credits, level ORDER BY credits, id) AS credits_dense_rank,
        DENSE_RANK() OVER (PARTITION BY dept ORDER BY credits, id) AS credits_dense_rank_2,
        MAX(credits) OVER w_level AS level_max_credits,
        ROUND(PERCENT_RANK() OVER (PARTITION BY dept ORDER BY credits, id), 3) AS credits_percent_rank
    FROM scoped
    WINDOW w_dept_by_credits AS (PARTITION BY dept ORDER BY credits, id), w_level AS (PARTITION BY level)
),
dept_stats AS (
    -- Max credits_dense_rank per dept
    SELECT dept, MAX(credits_dense_rank) AS max_credits_dense_rank
    FROM metrics
    GROUP BY dept
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_credits_dense_rank,
        ROUND(s.credits_dense_rank - g.max_credits_dense_rank, 2) AS credits_dense_rank_minus_max
    FROM metrics s
    JOIN dept_stats g ON g.dept = s.dept
    WHERE s.credits_dense_rank = g.max_credits_dense_rank
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        COUNT(*) AS enrollment_count_2,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2,
        e.first_enrollment_enrolled_at
    FROM outliers s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
)
SELECT *
FROM (
    SELECT credits, dept, prev3_credits
    FROM with_counts
) src
PIVOT (
    COUNT(prev3_credits)
    FOR dept IN ('CS', 'Biz', 'Art', 'Bio')
)
ORDER BY credits
