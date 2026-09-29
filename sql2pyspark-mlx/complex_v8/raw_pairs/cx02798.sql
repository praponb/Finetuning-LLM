WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 8 AND 14
      AND term = '2025T2'
),
joined_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_hour,
        st.cohort AS student_cohort,
        st.student_name,
        st.gpa AS student_gpa
    FROM recent_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
scored AS (
    -- Label and score rows
    SELECT *, DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM joined_student
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    course_id,
    COUNT(*) AS row_count,
    MIN(days_since_enrolled_at) AS min_days_since_enrolled_at,
    MAX(enrolled_at_hour) AS last_enrolled_at_hour
FROM scored
GROUP BY ROLLUP(grade_status, course_id)
ORDER BY grade_status NULLS LAST, course_id NULLS LAST
