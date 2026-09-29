WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score BETWEEN 2.2 AND 59.8
)
SELECT id AS key_id, enrolled_at, course_id
FROM scoped
UNION ALL
SELECT course_id AS key_id, enrolled_at, student_id AS course_id
FROM enrollments
WHERE term IS NOT NULL
  AND course_id >= 8
ORDER BY course_id DESC, enrolled_at
