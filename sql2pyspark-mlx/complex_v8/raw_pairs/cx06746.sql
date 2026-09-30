SELECT id
FROM students
INTERSECT
SELECT id
FROM students
WHERE enrolled_on < DATE '2025-07-03'
  AND cohort = 'C2021'
ORDER BY id
