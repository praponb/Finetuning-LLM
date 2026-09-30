SELECT
    'course' AS src,
    dept,
    level AS category,
    id AS entity_id
FROM courses
WHERE credits < 3
  AND LOWER(course_code) LIKE '%cs%'
UNION ALL
SELECT
    'enrollment' AS src,
    term AS dept,
    grade_status AS category,
    course_id AS entity_id
FROM enrollments
WHERE score <= 85.2
  AND grade_status NOT IN ('WITHDRAWN', 'INCOMPLETE')
ORDER BY dept DESC
