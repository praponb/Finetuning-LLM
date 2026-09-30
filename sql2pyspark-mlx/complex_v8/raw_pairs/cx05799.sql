SELECT course_code AS label, dept AS name
FROM courses
WHERE level IN ('advanced', 'core')
UNION ALL
SELECT course_code, level
FROM courses
WHERE credits <= 4
  AND UPPER(TRIM(course_code)) LIKE 'C%'
