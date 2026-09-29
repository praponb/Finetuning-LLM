WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM scoped s
    JOIN enrollments e ON e.student_id = s.student_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        enrolled_at,
        enrollment_id,
        enrollment_course_id,
        enrollment_enrolled_at,
        YEAR(enrollment_enrolled_at) AS enrollment_enrolled_at_year,
        TO_DATE(enrollment_enrolled_at) AS enrollment_enrolled_at_date
    FROM line_level
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.enrollment_id,
        s.enrollment_course_id,
        s.enrollment_enrolled_at,
        s.enrollment_enrolled_at_year,
        s.enrollment_enrolled_at_date,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM enriched s
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    enrolled_at,
    enrollment_course_id,
    enrollment_enrolled_at_year,
    enrollment_enrolled_at_date,
    student_gpa,
    student_is_international,
    ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(enrollment_enrolled_at)) / 3600, 1) AS hours_enrollment_enrolled_at_to_enrolled_at
FROM combined_details_student
ORDER BY enrollment_course_id DESC
