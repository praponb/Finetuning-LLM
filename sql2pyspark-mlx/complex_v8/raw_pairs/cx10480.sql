SELECT id, enrolled_on
FROM students
UNION ALL
SELECT id, enrolled_on
FROM students
WHERE enrolled_on >= DATE '2024-03-15'
  AND gpa < 3.2
UNION ALL
SELECT id, enrolled_on
FROM students
