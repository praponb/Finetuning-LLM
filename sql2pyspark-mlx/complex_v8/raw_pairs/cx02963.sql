WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref,
        CASE WHEN term IN ('2025T2', '2025T1') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE (score >= 11.1 OR student_id IS NOT NULL)
),
enriched_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_ref,
        s.term_group,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        grade_status_ref,
        course_credits,
        course_dept,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY course_id), 0), 4) AS course_credits_share_of_course_id,
        NTILE(10) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_decile,
        SIZE(COLLECT_SET(course_dept) OVER (PARTITION BY course_id)) AS distinct_course_dept_in_course_id,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id, YEAR(enrolled_at) ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        COUNT(*) OVER w_course_id AS rows_in_course_id,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at
    FROM enriched_course
    WINDOW w_course_id AS (PARTITION BY course_id)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM metrics s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        enrolled_at_decile,
        course_dept,
        course_id,
        ARRAY_SORT(COLLECT_SET(course_dept)) AS course_dept_list,
        SUM(minutes_since_prev_enrolled_at) AS total_minutes_since_prev_enrolled_at
    FROM summarized_rows
    WHERE minutes_since_prev_enrolled_at BETWEEN 1000.0 AND 4750.0
    GROUP BY enrolled_at_decile, course_dept, course_id
)
SELECT
    enrolled_at_decile,
    course_dept,
    SUBSTRING(TRIM(course_dept), 1, 4) AS course_dept_prefix
FROM per_group
WHERE total_minutes_since_prev_enrolled_at > 13500.0
ORDER BY course_dept_prefix NULLS LAST
