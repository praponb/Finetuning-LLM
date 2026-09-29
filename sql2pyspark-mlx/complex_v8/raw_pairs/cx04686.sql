WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label,
        UPPER(TRIM(grade_status)) AS grade_status_clean,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 8 AND 14
      AND course_id IS NOT NULL
),
with_details_student AS (
    -- Attach student attributes
    SELECT
        s.*,
        st.student_name,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa,
        c.level AS course_level,
        c.credits AS course_credits
    FROM base_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT
    student_is_international,
    course_credits,
    GROUPING(student_is_international) AS is_total_student_is_international,
    COUNT(DISTINCT term) AS distinct_terms
FROM with_details_student
GROUP BY GROUPING SETS ((student_is_international, course_credits), (student_is_international), ())
ORDER BY student_is_international NULLS LAST, course_credits NULLS LAST
