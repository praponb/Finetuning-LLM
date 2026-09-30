WITH valid_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        is_international
    FROM students
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM valid_students s
    WHERE s.id IN (
            SELECT e.student_id
            FROM enrollments e
            WHERE e.term = '2024T2'
        )
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.id) AS max_enrollment_score
    FROM screened s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_on, is_international, cohort, max_enrollment_score
    FROM profiled
) src
PIVOT (
    SUM(max_enrollment_score)
    FOR cohort IN ('C2024', 'C2022', 'C2023')
)
