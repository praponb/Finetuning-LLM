SELECT cohort
FROM students
WHERE is_international = TRUE
EXCEPT
SELECT major
FROM students
WHERE enrolled_on < DATE '2025-07-03'
  AND major = 'CS'
