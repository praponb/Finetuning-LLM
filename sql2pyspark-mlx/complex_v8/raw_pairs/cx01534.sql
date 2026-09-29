SELECT id
FROM students
WHERE student_name IS NOT NULL
  AND NOT is_international
EXCEPT ALL
SELECT id
FROM students
ORDER BY id
