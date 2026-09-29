SELECT
    id,
    student_name AS name,
    enrolled_on AS event_date,
    'student' AS record_type
FROM students
WHERE gpa < 3.9
UNION ALL
SELECT
    id,
    student_name AS name,
    enrolled_on AS event_date,
    'student_2' AS record_type
FROM students
WHERE major IN ('Biz', 'Math', 'CS')
  AND UPPER(TRIM(student_name)) LIKE 'C%'
