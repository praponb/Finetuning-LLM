SELECT id, major
FROM students
WHERE cohort = 'C2024'
  AND enrolled_on < DATE '2025-05-07'
EXCEPT
SELECT id, major
FROM students
WHERE gpa < 3.4
  AND enrolled_on IS NOT NULL
