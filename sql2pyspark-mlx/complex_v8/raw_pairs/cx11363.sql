WITH base_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        LENGTH(TRIM(dept)) AS dept_len
    FROM courses
    WHERE credits < 3
      AND level = 'advanced'
)
SELECT course_code AS label
FROM base_courses
MINUS
SELECT course_code
FROM courses
WHERE credits >= 2
ORDER BY label DESC
