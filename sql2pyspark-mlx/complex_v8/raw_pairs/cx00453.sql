WITH valid AS (
    -- Filter students
    SELECT
        id,
        cohort,
        major,
        gpa,
        is_international,
        MONTH(enrolled_on) AS enrolled_on_month
    FROM students
    WHERE YEAR(enrolled_on) = 2025
      AND UPPER(TRIM(student_name)) LIKE 'I%'
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.grade_status = 'INCOMPLETE'
              AND e.term = '2024T1'
        )
      AND s.gpa < 3.2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT cohort, enrolled_on_month, major, gpa
    FROM qualified
) src
PIVOT (
    MIN(gpa)
    FOR major IN ('Art', 'CS')
)
ORDER BY cohort, enrolled_on_month
