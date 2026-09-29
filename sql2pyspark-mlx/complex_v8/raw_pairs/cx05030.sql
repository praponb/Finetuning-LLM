SELECT
    id AS qty,
    enrolled_on AS day,
    gpa,
    'student' AS src
FROM students
UNION ALL
SELECT
    id AS qty,
    enrolled_on AS day,
    gpa,
    'student_2' AS src
FROM students
UNION ALL
SELECT
    id AS qty,
    enrolled_on AS day,
    gpa,
    'student_3' AS src
FROM students
ORDER BY qty DESC
