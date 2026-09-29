SELECT id, enrolled_on
FROM students
WHERE gpa < 3.4
  AND LOWER(student_name) LIKE '% s%'
UNION
SELECT id, enrolled_on
FROM students
WHERE is_international = TRUE
  AND gpa BETWEEN 2.7 AND 3.2
