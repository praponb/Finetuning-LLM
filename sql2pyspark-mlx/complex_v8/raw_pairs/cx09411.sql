SELECT grade_status, course_id AS ref_id, 'enrollment' AS record_type
FROM enrollments
WHERE student_id IS NOT NULL
UNION
SELECT major, id, 'student' AS record_type
FROM students
