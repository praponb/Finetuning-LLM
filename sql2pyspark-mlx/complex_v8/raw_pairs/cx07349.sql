SELECT id AS qty, dept AS category
FROM courses
WHERE credits >= 2
INTERSECT ALL
SELECT id, student_name
FROM students
WHERE enrolled_on BETWEEN DATE '2024-01-24' AND DATE '2025-07-03'
