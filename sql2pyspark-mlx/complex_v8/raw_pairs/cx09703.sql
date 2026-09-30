WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE course_id IS NOT NULL
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        st.enrolled_on AS student_enrolled_on,
        st.student_name,
        st.is_international AS student_is_international
    FROM cleaned s
    LEFT JOIN students st ON st.id = s.student_id AND st.enrolled_on < DATE '2025-07-03'
),
enriched AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.student_enrolled_on,
        s.student_is_international,
        c.course_code,
        c.level AS course_level
    FROM joined_student s
    JOIN courses c ON c.id = s.course_id AND UPPER(TRIM(c.course_code)) LIKE 'C%'
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    student_is_international,
    course_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS n_rows
FROM enriched
GROUP BY GROUPING SETS ((grade_status, student_is_international, course_id), (grade_status))
ORDER BY grade_status NULLS LAST, student_is_international NULLS LAST, course_id NULLS LAST
