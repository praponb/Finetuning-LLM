WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE term IS NOT NULL
      AND grade_status = 'INCOMPLETE'
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND MONTH(e.enrolled_at) IN (1, 2)
        )
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        term,
        grade_status_len,
        SUM(grade_status_len) AS total_grade_status_len,
        COUNT(*) AS n_rows
    FROM without_matches
    GROUP BY student_id, term, grade_status_len
    HAVING COUNT(*) >= 2
),
enriched_student AS (
    -- Join students
    SELECT
        s.student_id,
        s.term,
        s.grade_status_len,
        s.total_grade_status_len,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM totals s
    LEFT JOIN students st ON st.id = s.student_id AND st.is_international = TRUE
)
SELECT
    student_id,
    grade_status_len,
    student_is_international,
    student_gpa,
    ABS(total_grade_status_len - 39) AS total_grade_status_len_distance
FROM enriched_student
ORDER BY total_grade_status_len_distance DESC
