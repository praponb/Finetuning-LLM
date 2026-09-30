WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score <= 59.7
      AND student_id IS NOT NULL
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        st.gpa AS student_gpa,
        st.enrolled_on AS student_enrolled_on,
        st.student_name,
        c.course_code
    FROM scoped_enrollments s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
enriched_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.student_enrolled_on,
        s.student_name,
        s.course_code,
        c.course_code AS course_course_code
    FROM enriched s
    LEFT JOIN courses c ON c.id = s.course_id AND c.credits <= 3
)
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    student_enrolled_on,
    student_name,
    course_code,
    DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
    CONCAT_WS(' / ', grade_status, course_code) AS grade_status_course_code_label
FROM enriched_course
ORDER BY course_id
