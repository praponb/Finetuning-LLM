SELECT student_id
FROM enrollments
WHERE term = '2024T2'
INTERSECT ALL
SELECT student_id
FROM enrollments
WHERE grade_status IN ('PASS', 'INCOMPLETE')
