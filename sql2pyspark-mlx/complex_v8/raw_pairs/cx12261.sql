SELECT major, enrolled_on
FROM students
WHERE LENGTH(TRIM(student_name)) >= 7
UNION ALL
SELECT student_name, enrolled_on
FROM students
WHERE UPPER(TRIM(student_name)) LIKE 'A%'
  AND cohort IS NOT NULL
