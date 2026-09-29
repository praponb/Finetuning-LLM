SELECT credits AS key_id, course_code
FROM courses
WHERE dept = 'Biz'
  AND credits <= 3
MINUS
SELECT id AS key_id, dept AS course_code
FROM courses
ORDER BY course_code
