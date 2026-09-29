WITH scoped AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        CONCAT(dept, '#', CAST(id AS STRING)) AS dept_ref
    FROM courses
    WHERE credits <= 3
      AND dept IN ('CS', 'Math')
)
SELECT id AS entity_id, credits AS key_id
FROM scoped
UNION ALL
SELECT course_id, student_id
FROM enrollments
WHERE student_id >= 14
  AND score > 36.6
ORDER BY key_id DESC
