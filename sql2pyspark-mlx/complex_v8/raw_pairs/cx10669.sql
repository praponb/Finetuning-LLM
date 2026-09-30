WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        INITCAP(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE student_id <= 19
      AND score <= 63.9
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
              AND e.score BETWEEN 5.7 AND 80.2
        )
      AND s.enrolled_at_date < DATE '2025-01-01'
)
SELECT
    term,
    student_id,
    grade_status_or_unknown,
    GROUPING(term) AS is_total_term,
    COUNT(*) AS row_count,
    ROUND(AVG(score), 2) AS avg_score
FROM without_matches
GROUP BY GROUPING SETS ((term, student_id, grade_status_or_unknown), (term), (student_id), ())
ORDER BY term NULLS LAST, student_id NULLS LAST, grade_status_or_unknown NULLS LAST
