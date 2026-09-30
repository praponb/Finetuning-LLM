WITH scoped_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN credits >= 3 THEN 'HIGH' WHEN credits >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS credits_tier
    FROM courses
    WHERE level = 'core'
)
SELECT
    id,
    credits_tier,
    credits,
    'course' AS record_type
FROM scoped_courses
WHERE LENGTH(TRIM(course_code)) >= 5
  AND level <> 'core'
UNION ALL
SELECT
    credits,
    level,
    id,
    'course_2' AS record_type
FROM courses
WHERE course_code IS NOT NULL
ORDER BY credits DESC
