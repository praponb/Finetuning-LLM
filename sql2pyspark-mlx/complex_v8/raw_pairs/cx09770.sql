WITH recent AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled
    FROM courses
    WHERE LENGTH(TRIM(course_code)) >= 5
      AND level IN ('intro', 'core')
)
SELECT credits, dept AS category
FROM recent
INTERSECT ALL
SELECT id, major
FROM students
ORDER BY category, credits
