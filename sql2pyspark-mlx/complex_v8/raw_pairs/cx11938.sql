SELECT student_name, enrolled_on
FROM students
WHERE is_international = TRUE
UNION ALL
SELECT major AS student_name, enrolled_on
FROM students
WHERE UPPER(TRIM(student_name)) LIKE 'J%'
  AND cohort = 'C2023'
