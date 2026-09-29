WITH recent AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        level
    FROM courses
    WHERE LOWER(course_code) LIKE '%01%'
)
SELECT id
FROM recent
WHERE level = 'advanced'
EXCEPT ALL
SELECT id
FROM courses
WHERE UPPER(TRIM(course_code)) LIKE 'M%'
  AND dept = 'Bio'
