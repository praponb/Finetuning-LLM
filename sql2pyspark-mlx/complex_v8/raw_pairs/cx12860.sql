WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('-', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE student_id >= 16
      AND course_id IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa,
        st.student_name
    FROM prepared s
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT
    grade_status,
    student_id,
    term,
    GROUPING(grade_status) AS is_total_grade_status,
    COUNT(DISTINCT grade_status) AS distinct_grade_status
FROM with_parent
GROUP BY GROUPING SETS ((grade_status, student_id, term), (grade_status), (student_id), ())
