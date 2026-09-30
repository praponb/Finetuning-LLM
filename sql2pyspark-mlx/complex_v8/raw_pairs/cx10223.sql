WITH base AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa
    FROM students
    WHERE is_international = FALSE
      AND enrolled_on < DATE '2025-07-18'
)
SELECT cohort
FROM base
WHERE YEAR(enrolled_on) = 2025
INTERSECT
SELECT grade_status AS cohort
FROM enrollments
WHERE course_id <= 5
  AND term IS NOT NULL
ORDER BY cohort DESC
