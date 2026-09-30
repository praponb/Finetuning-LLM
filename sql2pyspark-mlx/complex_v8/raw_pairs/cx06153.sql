WITH recent AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug
    FROM courses
)
SELECT course_code AS name
FROM recent
WHERE dept = 'Bio'
  AND course_code IS NOT NULL
EXCEPT
SELECT cohort AS name
FROM students
WHERE is_international = TRUE
