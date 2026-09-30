WITH valid AS (
    -- Filter courses
    SELECT
        id,
        dept,
        credits,
        level,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug
    FROM courses
    WHERE level = 'advanced'
)
SELECT
    credits,
    course_code_slug,
    id AS ref_id,
    'course' AS origin
FROM valid
WHERE course_code_slug IS NOT NULL
  AND dept = 'Bio'
UNION ALL
SELECT
    id AS credits,
    level AS course_code_slug,
    credits AS ref_id,
    'course_2' AS origin
FROM courses
ORDER BY credits
