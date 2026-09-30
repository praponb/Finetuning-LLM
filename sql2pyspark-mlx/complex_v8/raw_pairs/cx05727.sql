WITH prepared_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        credits,
        level
    FROM courses
    WHERE credits <= 4
      AND dept IN ('Bio', 'Biz', 'Art')
)
SELECT 'course' AS origin, credits AS key_id, id
FROM prepared_courses
WHERE level = 'advanced'
  AND credits < 3
UNION ALL
SELECT 'course_2' AS origin, id AS key_id, credits AS id
FROM courses
WHERE dept IS NOT NULL
  AND level = 'intro'
ORDER BY origin
