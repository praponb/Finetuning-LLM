SELECT course_id AS ref_id, grade_status AS label
FROM enrollments
EXCEPT
SELECT student_id, term
FROM enrollments
