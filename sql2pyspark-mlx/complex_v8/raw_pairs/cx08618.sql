WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        MONTH(enrolled_at) AS enrolled_at_month,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE student_id <= 19
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM prepared_enrollments s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
            WHERE e.term IS NOT NULL
        )
      AND s.student_id IS NOT NULL
),
combined_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.enrolled_at_month,
        s.grade_status_slug,
        c.course_code,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM screened s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    enrolled_at_month,
    grade_status_slug,
    course_code,
    student_is_international,
    student_gpa - enrolled_at_month AS student_gpa_minus_enrolled_at_month,
    COALESCE(enrolled_at_month, 0) AS enrolled_at_month_filled
FROM combined_details
ORDER BY enrolled_at_month DESC
