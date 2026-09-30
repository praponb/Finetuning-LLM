SELECT course_code AS name
FROM courses
WHERE credits >= 2
  AND level = 'core'
INTERSECT ALL
SELECT course_code
FROM courses
ORDER BY name
