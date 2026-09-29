WITH base_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        level,
        CONCAT(course_code, '#', CAST(credits AS STRING)) AS course_code_ref,
        COALESCE(dept, 'UNKNOWN') AS dept_or_unknown,
        CASE WHEN dept IN ('Math', 'Bio') THEN 'group_a' WHEN dept = 'Biz' THEN 'group_b' ELSE 'other' END AS dept_group
    FROM courses
    WHERE (credits < 3 OR UPPER(TRIM(course_code)) LIKE 'M%')
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(score) AS min_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM base_courses s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.score <= 78.8
        )
      AND s.level IS NOT NULL
),
per_group AS (
    -- Summarise per group
    SELECT
        enrollment_count,
        dept_group,
        dept,
        MAX(min_enrollment_score) AS max_min_enrollment_score,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT dept_group) AS distinct_dept_groups,
        SUM(min_enrollment_score) AS total_min_enrollment_score
    FROM with_activity
    GROUP BY enrollment_count, dept_group, dept
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(enrollment_count - 4) AS enrollment_count_distance,
        LENGTH(TRIM(dept_group)) AS dept_group_len
    FROM per_group
    WHERE dept_group = 'other'
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(dept_group_len, n_rows) AS greatest_dept_group_len_n_rows,
        COALESCE(dept, 'UNKNOWN') AS dept_or_unknown,
        CASE WHEN dept_group_len >= 11 THEN 'large' WHEN dept_group_len >= 7 THEN 'medium' ELSE 'small' END AS dept_group_len_tier
    FROM labeled
    WHERE n_rows < 11
),
dept_group_len_tier_stats AS (
    -- Avg n_rows per dept_group_len_tier
    SELECT dept_group_len_tier, AVG(n_rows) AS avg_n_rows
    FROM annotated
    GROUP BY dept_group_len_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_n_rows, ROUND(s.n_rows - g.avg_n_rows, 2) AS n_rows_minus_avg
    FROM annotated s
    JOIN dept_group_len_tier_stats g ON g.dept_group_len_tier = s.dept_group_len_tier
    WHERE s.n_rows > g.avg_n_rows
)
-- Final projection
SELECT
    enrollment_count,
    dept_group,
    max_min_enrollment_score,
    n_rows,
    total_min_enrollment_score,
    greatest_dept_group_len_n_rows,
    dept_or_unknown,
    dept_group_len_tier,
    avg_n_rows,
    n_rows_minus_avg,
    COALESCE(dept_group, 'UNKNOWN') AS dept_group_or_unknown
FROM compared
ORDER BY greatest_dept_group_len_n_rows DESC, enrollment_count DESC
