WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status <> 'PASS'
)
SELECT student_id AS ref_id
FROM filtered
WHERE student_id IS NOT NULL
  AND course_id >= 7
INTERSECT
SELECT student_id AS ref_id
FROM enrollments
WHERE enrolled_at < TIMESTAMP '2025-03-14 07:00:00'
  AND course_id IS NOT NULL
