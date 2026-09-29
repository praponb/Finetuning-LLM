SELECT level
FROM courses
WHERE LOWER(course_code) LIKE '%s3%'
  AND level <> 'advanced'
INTERSECT
SELECT level
FROM courses
ORDER BY level DESC
