SELECT major AS name
FROM students
WHERE major = 'Math'
EXCEPT
SELECT course_code AS name
FROM courses
WHERE dept = 'Math'
ORDER BY name DESC
