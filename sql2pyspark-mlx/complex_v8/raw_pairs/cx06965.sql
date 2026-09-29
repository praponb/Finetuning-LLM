WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.term_prefix,
        c.course_code,
        c.dept AS course_dept,
        st.student_name
    FROM prepared_enrollments s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        grade_status,
        term_prefix,
        course_code,
        course_dept,
        student_name,
        COALESCE(course_dept, 'UNKNOWN') AS course_dept_or_unknown
    FROM with_details_course
)
SELECT
    id,
    student_id,
    term_prefix,
    course_dept,
    student_name,
    course_dept_or_unknown,
    CASE WHEN course_dept IN ('Bio', 'CS') THEN 'group_a' WHEN course_dept = 'Biz' THEN 'group_b' ELSE 'other' END AS course_dept_group
FROM derived
ORDER BY student_id DESC
