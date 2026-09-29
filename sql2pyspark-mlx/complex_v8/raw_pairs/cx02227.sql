SELECT course_code AS category
FROM courses
EXCEPT
SELECT term AS category
FROM enrollments
