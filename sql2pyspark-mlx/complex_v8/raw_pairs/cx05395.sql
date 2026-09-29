SELECT id AS key_id, dept
FROM courses
WHERE LENGTH(TRIM(course_code)) >= 4
  AND dept NOT IN ('Art', 'Biz')
EXCEPT
SELECT id AS key_id, level AS dept
FROM courses
ORDER BY dept, key_id
