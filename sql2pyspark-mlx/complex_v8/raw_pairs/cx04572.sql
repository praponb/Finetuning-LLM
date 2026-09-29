SELECT student_name, cohort AS label, enrolled_on AS day
FROM students
WHERE cohort = 'C2022'
  AND is_international = FALSE
UNION
SELECT cohort, major, enrolled_on
FROM students
WHERE gpa > 2.1
  AND cohort = 'C2023'
ORDER BY student_name, label
