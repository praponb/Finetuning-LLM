SELECT grade_status AS category, term AS name
FROM enrollments
UNION ALL
SELECT course_code, level
FROM courses
UNION ALL
SELECT course_code, dept
FROM courses
WHERE level IS NOT NULL
  AND credits < 3
ORDER BY name, category
