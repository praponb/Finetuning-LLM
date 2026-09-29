WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE (course_id >= 9 OR term IS NOT NULL)
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.days_since_enrolled_at,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM base_enrollments s
    JOIN courses c ON c.id = s.course_id AND UPPER(TRIM(c.course_code)) LIKE 'C%'
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.course_credits,
        c.level AS course_level,
        c.dept AS course_dept_2
    FROM joined_course s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    course_dept_2,
    student_id,
    course_credits,
    GROUPING(course_dept_2) AS is_total_course_dept_2,
    COUNT(*) FILTER (WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN', 'FAIL')) AS n_grade_status_in_incomplete_w,
    COUNT(*) AS n_rows,
    SUM(CASE WHEN term IS NOT NULL THEN score ELSE 0 END) AS score_term_is_not_null
FROM with_parent_course
GROUP BY ROLLUP(course_dept_2, student_id, course_credits)
ORDER BY course_dept_2 NULLS LAST, student_id NULLS LAST, course_credits NULLS LAST
