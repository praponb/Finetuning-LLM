SELECT id, major
FROM students
WHERE UPPER(TRIM(student_name)) LIKE 'J%'
EXCEPT
SELECT id, student_name AS major
FROM students
ORDER BY id DESC, major
