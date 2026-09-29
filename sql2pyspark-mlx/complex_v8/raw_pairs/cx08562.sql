SELECT id
FROM students
EXCEPT ALL
SELECT id
FROM students
WHERE enrolled_on < DATE '2025-07-18'
  AND cohort NOT IN ('C2022', 'C2023')
ORDER BY id DESC
