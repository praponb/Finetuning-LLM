SELECT id, cohort
FROM students
WHERE gpa <= 3.4
  AND cohort IS NOT NULL
INTERSECT
SELECT id, student_name AS cohort
FROM students
