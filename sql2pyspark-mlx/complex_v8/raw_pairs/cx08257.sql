SELECT id AS ref_id, student_name
FROM students
UNION ALL
SELECT credits AS ref_id, dept AS student_name
FROM courses
WHERE dept = 'Math'
ORDER BY ref_id DESC, student_name
