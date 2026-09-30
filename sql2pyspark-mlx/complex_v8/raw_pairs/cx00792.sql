WITH base AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
    WHERE term <> '2025T2'
      AND course_id <= 8
)
SELECT grade_status
FROM base
WHERE student_id <= 10
INTERSECT
SELECT cohort
FROM students
