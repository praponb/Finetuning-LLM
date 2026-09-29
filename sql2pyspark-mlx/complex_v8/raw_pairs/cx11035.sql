WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term IS NOT NULL
      AND enrolled_at < TIMESTAMP '2025-02-18 11:00:00'
),
with_parent_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        st.student_name
    FROM cleaned_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM with_parent_student s
    JOIN enrollments e ON e.course_id = s.course_id
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_children_enrollments s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
        )
),
combined_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_student_id,
        s.enrollment_grade_status,
        s.enrollment_enrolled_at,
        st.gpa AS student_gpa,
        st.student_name,
        st.enrolled_on AS student_enrolled_on,
        e.term AS enrollment_term_2,
        e.score AS enrollment_score
    FROM qualified s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
    WHERE st.gpa >= 2.1
)
SELECT
    id,
    student_id,
    grade_status,
    enrollment_id,
    enrollment_term,
    enrollment_student_id,
    enrollment_grade_status,
    enrollment_enrolled_at,
    student_gpa,
    student_enrolled_on,
    enrollment_term_2,
    enrollment_score
FROM combined_details
ORDER BY enrollment_term
