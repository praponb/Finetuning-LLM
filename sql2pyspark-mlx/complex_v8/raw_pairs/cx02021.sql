WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE dept IN ('Bio', 'CS')
      AND level IN ('core', 'advanced')
)
SELECT id
FROM prepared
EXCEPT
SELECT credits AS id
FROM courses
ORDER BY id
