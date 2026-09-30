WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
combined_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa,
        st.major AS student_major
    FROM prepared_enrollments s
    JOIN students st ON st.id = s.student_id
),
joined AS (
    -- Attach student attributes
    SELECT s.*, st.cohort AS student_cohort
    FROM combined_details_student s
    LEFT JOIN students st ON st.id = s.student_id AND st.is_international = FALSE
),
labeled AS (
    -- Label and score rows
    SELECT *, ROUND(student_gpa * 1.1, 2) AS student_gpa_adj
    FROM joined
    WHERE course_id <= 10
)
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    student_is_international,
    student_gpa,
    CONCAT(student_major, '#', CAST(student_id AS STRING)) AS student_major_ref
FROM labeled
ORDER BY term, student_gpa
