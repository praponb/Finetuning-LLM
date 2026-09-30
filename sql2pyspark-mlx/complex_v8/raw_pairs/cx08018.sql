WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        SUBSTRING(TRIM(level), 1, 2) AS level_prefix
    FROM courses
)
SELECT id AS qty, credits AS entity_id, level_prefix
FROM cleaned
UNION
SELECT id, credits, level
FROM courses
WHERE LOWER(course_code) LIKE '%a1%'
  AND level = 'advanced'
