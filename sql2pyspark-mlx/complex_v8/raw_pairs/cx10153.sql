WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
combined_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international,
        st.student_name
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    term,
    grade_status,
    student_gpa,
    student_is_international,
    student_name,
    COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
    COALESCE(student_gpa, 0.0) AS student_gpa_filled
FROM combined_details_student
WHERE student_gpa <= 3.2
ORDER BY student_gpa_filled NULLS LAST
