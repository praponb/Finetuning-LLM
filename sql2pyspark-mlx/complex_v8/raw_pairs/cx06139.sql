WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-25 11:00:00'
),
with_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        c.level AS course_level,
        st.student_name
    FROM scoped s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.level IN ('intro', 'core')
),
combined_details AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.course_level,
        s.student_name,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM with_details_course s
    JOIN courses c ON c.id = s.course_id AND c.credits < 4
    WHERE c.credits BETWEEN 2 AND 3
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM combined_details s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.score <= 74.9
              AND e.grade_status IN ('FAIL', 'WITHDRAWN', 'INCOMPLETE')
        )
      AND s.course_credits BETWEEN 1 AND 3
),
latest_per_key AS (
    -- Keep the latest row per term, student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        course_level,
        student_name,
        course_credits,
        course_dept
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, student_id ORDER BY course_credits DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.course_level,
        s.student_name,
        c.credits AS course_credits_2,
        c.dept AS course_dept_2,
        c.level AS course_level_2
    FROM latest_per_key s
    JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    course_level,
    course_credits_2,
    course_dept_2,
    DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter,
    COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
FROM joined_course
ORDER BY student_id DESC NULLS FIRST
