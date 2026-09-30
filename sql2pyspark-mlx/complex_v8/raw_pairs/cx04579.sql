SELECT
    student_id AS entity_id,
    term AS name,
    score AS metric,
    'enrollment' AS source
FROM enrollments
UNION
SELECT
    student_id,
    term,
    score,
    'enrollment_2' AS source
FROM enrollments
WHERE score <= 59.8
  AND course_id IS NOT NULL
ORDER BY entity_id, source
