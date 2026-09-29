SELECT id
FROM students
WHERE cohort <> 'C2021'
INTERSECT
SELECT id
FROM courses
WHERE UPPER(TRIM(course_code)) LIKE 'B%'
  AND credits BETWEEN 2 AND 3
ORDER BY id
