WITH valid AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        enrolled_on,
        gpa,
        ADD_MONTHS(enrolled_on, 12) AS enrolled_on_plus_12m
    FROM students
    WHERE cohort = 'C2024'
      AND UPPER(TRIM(student_name)) LIKE 'L%'
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.course_id >= 3
        )
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.enrolled_at >= TIMESTAMP '2025-01-25 13:00:00'
        )
)
SELECT
    id,
    student_name,
    enrolled_on,
    gpa,
    enrolled_on_plus_12m
FROM filtered
WHERE enrolled_on_plus_12m IS NOT NULL
ORDER BY enrolled_on_plus_12m DESC
