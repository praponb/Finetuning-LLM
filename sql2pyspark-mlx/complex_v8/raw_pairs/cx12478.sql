WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(TRIM(grade_status)) AS grade_status_clean,
        MONTH(enrolled_at) AS enrolled_at_month
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND HOUR(enrolled_at) BETWEEN 8 AND 13
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status = 'INCOMPLETE'
        )
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.enrolled_at >= TIMESTAMP '2025-01-07 11:00:00'
              AND e.score < 57.6
        )
)
SELECT *
FROM (
    SELECT student_id, term, grade_status, enrolled_at_month
    FROM filtered
) src
PIVOT (
    SUM(enrolled_at_month)
    FOR grade_status IN ('INCOMPLETE', 'FAIL')
)
ORDER BY student_id, term
